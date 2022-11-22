/**
 * This file is part of the CernVM File System.
 *
 * It provide a basic mock for AbstractSyncMediator
 */

#ifndef TEST_UNITTESTS_MOCK_M_SYNC_MEDIATOR_H_
#define TEST_UNITTESTS_MOCK_M_SYNC_MEDIATOR_H_

#include <string>

#include "sync_mediator.h"
#include "util/shared_ptr.h"

namespace publish {

class MockSyncMediator : public AbstractSyncMediator {
 public:
  MockSyncMediator() : n_register(0) {}
  virtual ~MockSyncMediator() {}

  virtual void RegisterUnionEngine(SyncUnion * /* engine */) { n_register++; }

  virtual void Add(SharedPtr<SyncItem> /* entry */) {}
  virtual void Touch(SharedPtr<SyncItem> /* entry */) {}
  virtual void Remove(SharedPtr<SyncItem> /* entry */) {}
  virtual void Replace(SharedPtr<SyncItem> /* entry */) {}
  virtual void Clone(const std::string /* from */,
                     const std::string /* to */) {}

  virtual void AddUnmaterializedDirectory(SharedPtr<SyncItem> /* entry */) {}

  virtual void EnterDirectory(SharedPtr<SyncItem> /* entry */) {}
  virtual void LeaveDirectory(SharedPtr<SyncItem> /* entry */) {}

  virtual bool Commit(manifest::Manifest * /* manifest */) { return true; }

  virtual bool IsExternalData() const { return false; }
  virtual bool IsDirectIo() const { return false; }
  virtual zlib::Algorithms GetCompressionAlgorithm() const {
    return zlib::kZlibDefault;
  }

  int n_register;
};  // class MockSyncMediator

}  // namespace publish

#endif  // TEST_UNITTESTS_MOCK_M_SYNC_MEDIATOR_H_
