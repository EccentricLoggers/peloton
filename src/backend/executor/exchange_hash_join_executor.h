#pragma once

#include <deque>
#include <vector>

#include "backend/executor/abstract_join_executor.h"
#include "backend/executor/exchange_hash_executor.h"
#include "backend/executor/hash_executor.h"
#include "backend/planner/hash_join_plan.h"

#include <atomic>
#include "backend/common/barrier.h"
#include "backend/common/thread_manager.h"
#include "boost/lockfree/queue.hpp"

namespace peloton {
namespace executor {

class ConcurrentOidSet {
 public:
  ConcurrentOidSet() {}

  ConcurrentOidSet(ConcurrentOidSet &&that)
      : container_(std::move(that.container_)) {}
  ConcurrentOidSet(const ConcurrentOidSet &that) = delete;

  ConcurrentOidSet &operator=(const ConcurrentOidSet &that) = delete;
  ConcurrentOidSet &operator=(ConcurrentOidSet &&that) = delete;

  size_t MUTEX_CNT = 16;
  std::array<std::mutex, 16> mtx_list_;
  std::unordered_set<oid_t> container_;

  void Erase(oid_t key) {
    int mutex_id = key % MUTEX_CNT;
    mtx_list_[mutex_id].lock();
    container_.erase(key);
    mtx_list_[mutex_id].unlock();
  }

  bool Empty() { return container_.empty(); }
};

typedef std::uint_least32_t thread_no;

class PesudoBarrier {
 public:
  void Release() {
    std::lock_guard<std::mutex> lock(mutex_);
    ++count_;
    assert(count_ <= total_);
    if (count_ == total_) {
      done = true;
    }
  }
  PesudoBarrier() : total_(0) {}
  void SetTotal(thread_no new_total) { total_ = new_total; }
  bool IsDone() { return done; }
  bool IsNoNeedToDo() { return no_need_to_do_; }
  void SetNeedToDo(bool value) { no_need_to_do_ = value; }

 private:
  thread_no total_;
  bool done = false;
  std::mutex mutex_;
  size_t count_ = 0;
  bool no_need_to_do_ = false;
};

typedef std::vector<ConcurrentOidSet> ExHashJoinRowSets;

class ExchangeHashJoinExecutor : public AbstractJoinExecutor {
  ExchangeHashJoinExecutor(const ExchangeHashJoinExecutor &) = delete;
  ExchangeHashJoinExecutor &operator=(const ExchangeHashJoinExecutor &) =
      delete;

 public:
  explicit ExchangeHashJoinExecutor(const planner::AbstractPlan *node,
                                    ExecutorContext *executor_context);

  std::vector<LogicalTile *> GetOutputs();

  ~ExchangeHashJoinExecutor() = default;

  void GetRightHashTable(Barrier *barrier);
  void GetLeftScanResult(Barrier *barrier);

  void Probe(std::atomic<thread_no> *no, PesudoBarrier *barrier);
  void UpdateLeftJoinRowSets();
  void UpdateRightJoinRowSets();

  // helper function to launch number worker threads, each of which will do
  // function
  static void LaunchWorkerThreads(size_t number,
                                  std::function<void()> function) {
    LOG_TRACE("LaunchWorkerThreads(%lu)", number);
    ThreadManager &tm = ThreadManager::GetInstance();
    for (size_t i = 0; i < number; ++i) tm.AddTask(function);
  }

  inline void RecordMatchedLeftRow(size_t tile_idx, oid_t row_idx) {
    switch (join_type_) {
      case JOIN_TYPE_LEFT:
      case JOIN_TYPE_OUTER:
        no_matching_left_row_sets_[tile_idx].erase(row_idx);
        break;
      default:
        break;
    }
  }

  /**
   * Record a matched right row, which should not be constructed
   * when building join outputs
   */
  inline void RecordMatchedRightRow(size_t tile_idx, oid_t row_idx) {
    switch (join_type_) {
      case JOIN_TYPE_RIGHT:
      case JOIN_TYPE_OUTER:
        exhj_no_matching_right_row_sets_[tile_idx].Erase(row_idx);
        break;
      default:
        break;
    }
  }

  void SetTaskNumPerThread(size_t num) { SIZE_PER_PARTITION = num; }

  std::chrono::time_point<std::chrono::system_clock> main_start;
  std::chrono::time_point<std::chrono::system_clock> main_end;
  PesudoBarrier probe_barrier_;

 protected:
  bool DInit();

  bool DExecute();

 private:
  ExchangeHashExecutor *hash_executor_ = nullptr;

  bool hashed_ = false;
  bool prepare_children_ = false;
  bool exec_outer_join_ = false;

  boost::lockfree::queue<LogicalTile *, boost::lockfree::capacity<1000>>
      lockfree_buffered_output_tiles;

  std::atomic<size_t> atomic_left_matching_idx;
  std::atomic<size_t> atomic_right_matching_idx;

  std::vector<std::unique_ptr<LogicalTile>> right_tiles_;

  bool no_need_to_probe_ = false;
  size_t SIZE_PER_PARTITION = 75;

  ExHashJoinRowSets exhj_no_matching_right_row_sets_;

  bool BuildRightJoinOutput();
  bool BuildLeftJoinOutput();
};

}  // namespace executor
}  // namespace peloton
