#pragma once

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <mutex>
#include <queue>

#include "backend/common/types.h"
#include "backend/executor/abstract_executor.h"
#include "backend/planner/abstract_scan_plan.h"

namespace peloton {
namespace executor {
class ExchangeSeqScanExecutor : public AbstractExecutor {
 public:
  ExchangeSeqScanExecutor(const ExchangeSeqScanExecutor &) = delete;
  ExchangeSeqScanExecutor &operator=(const ExchangeSeqScanExecutor &) = delete;
  ExchangeSeqScanExecutor(ExchangeSeqScanExecutor &&) = delete;
  ExchangeSeqScanExecutor &operator=(ExchangeSeqScanExecutor &&) = delete;

  explicit ExchangeSeqScanExecutor(const planner::AbstractPlan *node,
                                   ExecutorContext *executor_context)
      : AbstractExecutor(node, executor_context) {}

 protected:
  bool DInit();
  bool DExecute();
  void ScanOneTileGroup(const oid_t tile_group_itr, concurrency::Transaction *transaction);

 protected:
  /** @brief Selection predicate. */
  const expression::AbstractExpression *predicate_ = nullptr;

  /** @brief Columns from tile group to be added to logical tile output. */
  std::vector<oid_t> column_ids_;

  /** @brief Pointer to table to scan from. */
  storage::DataTable *target_table_ = nullptr;

  // whether we have finished scan
  bool done_ = false;
  // lock to protect result_ and finished_number_
  std::mutex result_lock_;
  // coordinate thread waits on this variable, wait for all tasks to finish
  std::condition_variable wait_cv_;
  std::queue<std::unique_ptr<LogicalTile>> result_;
  // total number of tile groups which is also the total number of tasks
  oid_t tile_group_number_ = 0;
  oid_t finished_number_ = 0;
};
}
}
