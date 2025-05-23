/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "c_repository.h"
#include "history.h"
#include "manifest.h"
#include "publish/repository.h"
#include "publish/settings.h"

using namespace std;  // NOLINT

namespace publish {

class T_Diff : public ::testing::Test {
 protected:
  virtual void SetUp() { }

 protected:
};

class DiffTester : public DiffListener {
 public:
  int n_add_;
  int n_rem_;
  int n_mod_;

  DiffTester() : n_add_(0), n_rem_(0), n_mod_(0) { }
  virtual ~DiffTester() { }
  virtual void OnInit(const history::History::Tag &from_tag,
                      const history::History::Tag &to_tag) { }

  virtual void OnStats(const catalog::DeltaCounters &delta) { }

  virtual void OnAdd(const std::string &path,
                     const catalog::DirectoryEntry &entry) {
    n_add_++;
  }

  virtual void OnRemove(const std::string &path,
                        const catalog::DirectoryEntry &entry) {
    n_rem_++;
  }

  virtual void OnModify(const std::string &path,
                        const catalog::DirectoryEntry &entry_from,
                        const catalog::DirectoryEntry &entry_to) {
    n_mod_++;
  }
};

TEST_F(T_Diff, Ident) {
  DiffTester diff_tester;
  Publisher *publisher = GetTestPublisher();
  history::History::Tag tag_from;
  tag_from.name = "from";
  tag_from.root_hash = publisher->manifest()->catalog_hash();
  history::History::Tag tag_to;
  tag_to.name = "to";
  tag_to.root_hash = publisher->manifest()->catalog_hash();
  std::vector<history::History::Tag> add_tags;
  add_tags.push_back(tag_from);
  add_tags.push_back(tag_to);
  std::vector<std::string> rm_tags;

  publisher->Transaction();
  publisher->EditTags(add_tags, rm_tags);
  publisher->Publish();

  Repository *repository = GetRepositoryFromPublisher(*publisher);
  delete publisher;

  repository->Diff("from", "to", &diff_tester);
  delete repository;

  EXPECT_EQ(0, diff_tester.n_add_);
  EXPECT_EQ(0, diff_tester.n_mod_);
  EXPECT_EQ(0, diff_tester.n_rem_);
}

}  // namespace publish
