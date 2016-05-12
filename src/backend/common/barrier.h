
#pragma once

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "backend/common/value.h"
#include "backend/executor/logical_tile.h"

namespace peloton {
namespace executor {

/*
 * A Barrier is used to synchronize a coordinator thread
 * with multiple worker threads.
 * A worker thread calls Release() when its work is done.
 * The coordinator thread calls Wait on a barrier.
 * When the Wait returns, it knows all the worker threads have
 * finished their jobs.
 */
class Barrier {
 public:
  typedef std::uint_least32_t thread_no;

  Barrier(thread_no total) : total_(total) {}
  Barrier(const Barrier &) = delete;
  Barrier(Barrier &&) = delete;
  Barrier &operator=(const Barrier &) = delete;

  void Release() {
    std::lock_guard<std::mutex> lock(mutex_);
    ++count_;
    assert(count_ <= total_);
    if (count_ == total_) condition_variable_.notify_one();
  }

  void Wait() {
    std::unique_lock<std::mutex> lock(mutex_);
    while (count_ < total_) condition_variable_.wait(lock);
  }

 private:
  // total number of worker threads
  const thread_no total_;
  std::mutex mutex_;
  std::condition_variable condition_variable_;
  size_t count_ = 0;
};
}
}
