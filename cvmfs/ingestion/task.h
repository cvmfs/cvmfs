/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_INGESTION_TASK_H_
#define CVMFS_INGESTION_TASK_H_

#include <pthread.h>

#include <cassert>
#include <vector>

#include "ingestion/tube.h"
#include "util/single_copy.h"

/**
 * Forward declaration of TubeConsumerGroup so that it can be used as a friend
 * class to TubeConsumer.
 */
template<typename ItemT>
class TubeConsumerGroup;


/**
 * Base class for threads that processes items from a tube one by one.  Concrete
 * implementations overwrite the Process() method.
 */
template <class ItemT>
class TubeConsumer : SingleCopy {
  friend class TubeConsumerGroup<ItemT>;

 public:
  virtual ~TubeConsumer() { }

 protected:
  explicit TubeConsumer(Tube<ItemT> *tube) : tube_(tube) { }
  virtual void Process(ItemT *item) = 0;
  virtual void OnTerminate() { }

  Tube<ItemT> *tube_;

 private:
  static void *MainConsumer(void *data) {
    TubeConsumer<ItemT> *consumer =
      reinterpret_cast<TubeConsumer<ItemT> *>(data);

    pthread_cleanup_push(CleanupConsumer, consumer);
    while (true) {
      ItemT *item = consumer->tube_->Pop();
      consumer->Process(item);
    }
    pthread_cleanup_pop(1);
    return NULL;
  }

  static void CleanupConsumer(void *data) {
    TubeConsumer<ItemT> *consumer =
      reinterpret_cast<TubeConsumer<ItemT> *>(data);
    consumer->OnTerminate();
  }
};


template <class ItemT>
class TubeConsumerGroup : SingleCopy {
 public:
  TubeConsumerGroup() : is_active_(false) { }

  ~TubeConsumerGroup() {
    for (unsigned i = 0; i < consumers_.size(); ++i)
      delete consumers_[i];
  }

  void TakeConsumer(TubeConsumer<ItemT> *consumer) {
    assert(!is_active_);
    consumers_.push_back(consumer);
  }

  void Spawn() {
    assert(!is_active_);
    unsigned N = consumers_.size();
    threads_.reserve(N);
    for (unsigned i = 0; i < N; ++i) {
      int retval = pthread_create(
        &threads_[i], NULL, TubeConsumer<ItemT>::MainConsumer, consumers_[i]);
      assert(retval == 0);
    }
    is_active_ = true;
  }

  void Terminate() {
    unsigned N = consumers_.size();
    for (unsigned i = 0; i < N; ++i) {
      int retval = pthread_cancel(threads_[i]);
      assert(retval == 0);
    }
    for (unsigned i = 0; i < N; ++i) {
      int retval = pthread_join(threads_[i], NULL);
      assert(retval == 0);
    }
  }

 private:
  bool is_active_;
  std::vector<TubeConsumer<ItemT> *> consumers_;
  std::vector<pthread_t> threads_;
};

#endif
