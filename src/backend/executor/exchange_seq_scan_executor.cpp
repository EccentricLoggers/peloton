#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <thread>
#include <utility>

#include "backend/common/logger.h"
#include "backend/common/thread_manager.h"
#include "backend/common/types.h"
#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/executor/exchange_seq_scan_executor.h"
#include "backend/executor/executor_context.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/executor/seq_scan_executor.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/container_tuple.h"
#include "backend/planner/seq_scan_plan.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile.h"
#include "backend/storage/tile_group_header.h"

namespace peloton {
namespace executor {
bool ExchangeSeqScanExecutor::DInit() {
  assert(children_.size() == 0 || children_.size() == 1);
  assert(executor_context_);

  // Grab data from plan node.
  const planner::SeqScanPlan &node = GetPlanNode<planner::SeqScanPlan>();
  predicate_ = node.GetPredicate();
  column_ids_ = std::move(node.GetColumnIds());

  target_table_ = node.GetTable();
  if (target_table_ != nullptr) {
    tile_group_number_ = target_table_->GetTileGroupCount();
    if (column_ids_.empty()) {
      column_ids_.resize(target_table_->GetSchema()->GetColumnCount());
      std::iota(column_ids_.begin(), column_ids_.end(), 0);
    }
  }

  return true;
}

/*
 * This method is not supposed to be called by multiple threads, even though
 * we are implementing a parallelized executor.
 */
bool ExchangeSeqScanExecutor::DExecute() {
  if (!done_) {
    // Scanning over a logical tile.
    if (children_.size() == 1) {
      LOG_TRACE("Exchange Sequential Scan Executor : 1 child ");

      assert(target_table_ == nullptr);
      assert(column_ids_.size() == 0);
      // In this case, just use one thread to pull data
      while (children_[0]->Execute()) {
        std::unique_ptr<LogicalTile> tile(children_[0]->GetOutput());
        if (predicate_ != nullptr) {
          // Invalidate tuples that don't satisfy the predicate.
          for (oid_t tuple_id : *tile) {
            expression::ContainerTuple<LogicalTile> tuple(tile.get(), tuple_id);
            if (predicate_->Evaluate(&tuple, nullptr, executor_context_)
                    .IsFalse()) {
              tile->RemoveVisibility(tuple_id);
            }
          }
        }
        if (0 == tile->GetTupleCount()) {  // Avoid returning empty tiles
          continue;
        }
        result_.emplace(tile.release());
      }
    }
    // Scanning a table
    else if (children_.size() == 0) {
      LOG_TRACE("Exchange Sequential Scan Executor : 0 child ");

      assert(target_table_ != nullptr);
      assert(target_table_->GetTileGroupCount() == tile_group_number_);
      assert(column_ids_.size() > 0);

      // In this case, wrap the scanning of each tile group into an individual
      // task
      // Break the whole execution into multiple tasks
      for (oid_t tile_group_itr = 0; tile_group_itr<tile_group_number_; ++tile_group_itr) {
        ThreadManager::GetInstance().AddTask(
            std::bind(&ExchangeSeqScanExecutor::ScanOneTileGroup, this, tile_group_itr,
                      concurrency::current_txn));
      }
      {
        // Wait for all tasks to be done
        std::unique_lock<std::mutex> lock(result_lock_);
        while (finished_number_ < tile_group_number_) wait_cv_.wait(lock);
      }
    }
    done_ = true;
  }
  // Having finished the entire scan
  // Return results
  assert(done_);
  assert(finished_number_ == tile_group_number_);
  if (result_.empty()) return false;
  SetOutput(result_.front().release());
  result_.pop();
  return true;
}

/*
 * Do a table scan on one tile group
 */
void ExchangeSeqScanExecutor::ScanOneTileGroup(
        const oid_t tile_group_itr, concurrency::Transaction *transaction) {
  /*
   * This part is basically the same as single thread version of sequential scan
   * executor, except
   * we didn't put any transaction isolation management code here.
   * There are two reasons for this:
   * 1. The data structure used in transaction manager is not thread-safe.
   *   By making transaction thread local
   * (src/backend/concurrency/transaction_manager.h),
   * the transaction manager implicitly assumes that each transaction uses only
   * one thread.
   * Therefore the inner data structure is not thread safe
   * (src/backend/concurrency/transaction.h).
   * 2. Depending on how many data actually read, this transaction manager might
   * be a big overhead.
   *   This is because for each record we read, the tm will put on entry in the
   * read set. For an OLAP query
   * which typically scans an entire large table, this is a big overhead both in
   * time and memory.
   * Therefore, we didn't use transaction manager here and expects this executor
   * only work on the portion of
   * table that has been transformed into DSM and is read-only.
   * This is also the reason why we didn't merge this part of code into
   * seq_scan_executor
   */
  concurrency::current_txn = transaction;
  auto &transaction_manager =
          concurrency::TransactionManagerFactory::GetInstance();
  auto tile_group = target_table_->GetTileGroup(tile_group_itr);
  auto tile_group_header = tile_group->GetHeader();
  oid_t active_tuple_count = tile_group->GetNextTupleSlot();

  // Construct position list by looping through tile group
  // and applying the predicate.
  std::vector<oid_t> position_list;

  for (oid_t tuple_id = 0; tuple_id < active_tuple_count; tuple_id++) {
    if (transaction_manager.IsVisible(tile_group_header, tuple_id)) {
      if(predicate_==nullptr) {
        position_list.push_back(tuple_id);
      }
      else {
        expression::ContainerTuple<storage::TileGroup> tuple(tile_group.get(),
                                                             tuple_id);
        auto eval =
                predicate_->Evaluate(&tuple, nullptr, executor_context_).IsTrue();
        if(eval==true) {
          position_list.push_back(tuple_id);
        }
      }
    }
  }

  {
    // Mark one task as done
    std::lock_guard<std::mutex> guard(result_lock_);
    // Don't return empty tiles
    if (!position_list.empty()) {
      // Construct logical tile.
      LogicalTile *logical_tile = LogicalTileFactory::GetTile();
      logical_tile->AddColumns(tile_group, column_ids_);
      logical_tile->AddPositionList(std::move(position_list));
      result_.emplace(logical_tile);
    }

    ++finished_number_;
    if (finished_number_ == tile_group_number_)
      // wake up coordinate thread
      wait_cv_.notify_one();
  }
}
}
}