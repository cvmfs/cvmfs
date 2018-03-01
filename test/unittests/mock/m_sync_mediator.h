/**
 * This file is part of the CernVM File System.
 *
 * It provide a basic mock for AbstractSyncMediator
 */

#ifndef TEST_UNITTESTS_MOCK_M_SYNC_MEDIATOR_H_
#define TEST_UNITTESTS_MOCK_M_SYNC_MEDIATOR_H_

#include <gmock/gmock.h>

#include "hash.h"
#include "sync_mediator.h"

using ::testing::Invoke;
using ::testing::_;
using ::testing::DefaultValue;
using ::testing::Return;
using ::testing::Property;

namespace publish {

class MockSyncMediator : public AbstractSyncMediator {
 public:
  MockSyncMediator() : AbstractSyncMediator() {}
  MockSyncMediator(catalog::WritableCatalogManager *catalog_manager,
                   const SyncParameters *params)
      : AbstractSyncMediator() {}

  ~MockSyncMediator() {}

  MOCK_METHOD1(RegisterUnionEngine, void(SyncUnion *engine));
  MOCK_METHOD1(Add, void(const SyncItem &entry));
  MOCK_METHOD1(Touch, void(const SyncItem &entry));
  MOCK_METHOD1(Remove, void(const SyncItem &entry));
  MOCK_METHOD1(Replace, void(const SyncItem &entry));
  MOCK_METHOD1(EnterDirectory, void(const SyncItem &entry));
  MOCK_METHOD1(LeaveDirectory, void(const SyncItem &entry));
  MOCK_METHOD1(Commit, bool(manifest::Manifest *manifest));
  MOCK_CONST_METHOD0(IsExternalData, bool());
  MOCK_CONST_METHOD0(GetCompressionAlgorithm, zlib::Algorithms());
};  // class MockSyncMediator

}  // namespace publish

#endif  // TEST_UNITTESTS_MOCK_M_SYNC_MEDIATOR_H_
