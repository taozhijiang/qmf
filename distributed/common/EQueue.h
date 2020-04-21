/*-
 * Copyright (c) 2020 taozhijiang@gmail.com
 *
 * Licensed under the BSD-3-Clause license, see LICENSE for full information.
 *
 */

#ifndef __DISTRIBUTED_COMMON_EQUEUE_H__
#define __DISTRIBUTED_COMMON_EQUEUE_H__

/**
 * This is a simple task-queue based on std::deque
 * it use mutex for thread-safe, and conditiona_variable for effective notify
 */

#include <vector>
#include <deque>

#include <mutex>
#include <condition_variable>
#include <chrono>
#include <functional>

namespace distributed {

template <typename T>
class EQueue {
 public:
  EQueue() = default;
  ~EQueue() = default;

  void PUSH(const T& t) {
    std::lock_guard<std::mutex> lock(lock_);
    items_.push_back(t);
    item_notify_.notify_one();
  }

  void POP(T& t) {
    t = POP();
  }

  T POP() {
    std::unique_lock<std::mutex> lock(lock_);
    while (items_.empty()) {
      item_notify_.wait(lock);
    }

    T t = items_.front();
    items_.pop_front();
    return t;
  }

  size_t TRY_POP(std::vector<T>& vec) {
    std::unique_lock<std::mutex> lock(lock_);

    if (items_.empty()) {
      return 0;
    }

    vec.clear();
    vec.assign(items_.begin(), items_.end());
    items_.clear();

    return vec.size();
  }

  size_t POP(std::vector<T>& vec, size_t max_count, uint64_t msec) {
    std::unique_lock<std::mutex> lock(lock_);

    // because wait_for may have spurious problem, so here we use wait_until
    // instead.

    auto now = std::chrono::system_clock::now();
    auto expire_tp = now + std::chrono::milliseconds(msec);

    // no_timeout wakeup by notify_all, notify_one, or spuriously
    // timeout    wakeup by timeout expiration
    while (items_.empty()) {

      // if timeout occurs, jump to the cheak final result

#if __cplusplus >= 201103L
      if (item_notify_.wait_until(lock, expire_tp) == std::cv_status::timeout) {
        break;
      }
#else
      if (!item_notify_.wait_until(lock, expire_tp)) {
        break;
      }
#endif
    }

    // all check here
    if (items_.empty()) {
      return 0;
    }

    size_t ret_count = 0;
    do {
      T t = items_.front();
      items_.pop_front();
      vec.emplace_back(t);
      ++ret_count;
    } while (ret_count < max_count && !items_.empty());

    return ret_count;
  }

  bool POP(T& t, uint64_t msec) {
    std::unique_lock<std::mutex> lock(lock_);

    auto now = std::chrono::system_clock::now();
    auto expire_tp = now + std::chrono::milliseconds(msec);

    // no_timeout wakeup by notify_all, notify_one, or spuriously
    // timeout    wakeup by timeout expiration
    while (items_.empty()) {

#if __cplusplus >= 201103L
      if (item_notify_.wait_until(lock, expire_tp) == std::cv_status::timeout) {
        break;
      }
#else
      if (!item_notify_.wait_until(lock, expire_tp)) {
        break;
      }
#endif
    }

    // all check here
    if (items_.empty()) {
      return false;
    }

    t = items_.front();
    items_.pop_front();
    return true;
  }

  // push when "t" does not exist
  bool UNIQUE_PUSH(const T& t) {
    std::lock_guard<std::mutex> lock(lock_);
    if (std::find(items_.begin(), items_.end(), t) == items_.end()) {
      items_.push_back(t);
      item_notify_.notify_one();
      return true;
    }
    return false;
  }

  size_t SIZE() {
    std::lock_guard<std::mutex> lock(lock_);
    return items_.size();
  }

  bool EMPTY() {
    std::lock_guard<std::mutex> lock(lock_);
    return items_.empty();
  }

  size_t SHRINK_FRONT(size_t sz) {
    std::lock_guard<std::mutex> lock(lock_);

    size_t orig_sz = items_.size();
    if (orig_sz <= sz)
      return 0;

    auto iter = items_.end() - sz;
    items_.erase(items_.begin(), iter);

    return orig_sz - items_.size();
  }

 private:
  std::mutex lock_;
  std::condition_variable item_notify_;

  std::deque<T> items_;
};

} // end namespace distributed

#endif // __DISTRIBUTED_COMMON_EQUEUE_H__
