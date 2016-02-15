/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "../../cvmfs/reflog.h"
#include "testutil.h"

template <class ReflogT>
class T_Reflog : public ::testing::Test {
 protected:
  static const char        sandbox[];
  static const std::string fqrn;

  typedef ReflogT Reflog;
  typedef std::map<std::string, MockReflog*> MockReflogMap;

 protected:
  virtual void SetUp() {
    if (NeedsSandbox()) {
      ASSERT_TRUE(MkdirDeep(string(sandbox), 0700))
                  << "failed to create sandbox";
    }

    prng_.InitSeed(42);
  }

  virtual void TearDown() {
    if (NeedsSandbox()) {
      const bool retval = RemoveTree(string(sandbox));
      ASSERT_TRUE(retval) << "failed to remove sandbox";
    }

          MockReflogMap::const_iterator i    = mock_reflogs_.begin();
    const MockReflogMap::const_iterator iend = mock_reflogs_.end();
    for (; i != iend; ++i) {
      delete i->second;
    }
    mock_reflogs_.clear();
  }

  std::string GetReflogFilename() const {
    std::string path;
    if (NeedsSandbox()) {
      path = CreateTempPath(string(sandbox) + "/reflog", 0600);
    } else {
      do {
        path = StringifyInt(prng_.Next(1234567));
      } while (mock_reflogs_.count(path) > 0);
    }
    CheckEmpty(path);
    return path;
  }

  bool NeedsSandbox() const {
    return NeedsSandbox(type<ReflogT>());
  }

  ReflogT* CreateReflog(const std::string &path) {
    return CreateReflog(type<ReflogT>(), path);
  }

  ReflogT* OpenReflog(const std::string &path) {
    return OpenReflog(type<ReflogT>(), path);
  }

  void CloseReflog(ReflogT *reflog) {
    return CloseReflog(type<ReflogT>(), reflog);
  }

 private:
  // type-based overloaded instantiation of Reflog object wrapper
  // Inspired from here:
  //   http://stackoverflow.com/questions/5512910/
  //          explicit-specialization-of-template-class-member-function
  template <typename T> struct type {};

  bool NeedsSandbox(const type<manifest::Reflog> type_specifier) const {
    return true;
  }

  bool NeedsSandbox(const type<MockReflog> type_specifier) const {
    return false;
  }

  ReflogT* CreateReflog(const type<manifest::Reflog>  type_specifier,
                        const std::string            &path) {
    return Reflog::Create(path, fqrn);
  }

  ReflogT* CreateReflog(const type<MockReflog>  type_specifier,
                        const std::string      &path) {
    MockReflog* reflog = MockReflog::Create(path, fqrn);
    mock_reflogs_[path] = reflog;
    return reflog;
  }

  ReflogT* OpenReflog(const type<manifest::Reflog>  type_specifier,
                      const std::string            &path) {
    return Reflog::Open(path);
  }

  ReflogT* OpenReflog(const type<MockReflog>  type_specifier,
                      const std::string      &path) {
    return (mock_reflogs_.count(path) > 0)
      ? mock_reflogs_[path]
      : NULL;
  }

  void CloseReflog(const type<manifest::Reflog>  type_specifier,
                   ReflogT                      *reflog) {
    delete reflog;
  }

  void CloseReflog(const type<MockReflog>  type_specifier,
                   ReflogT                *reflog) {
    // NOOP
  }

  void CheckEmpty(const std::string &str) const {
    ASSERT_FALSE(str.empty());
  }

 protected:
  mutable Prng prng_;

  MockReflogMap mock_reflogs_;
};

template <class ReflogT>
const char T_Reflog<ReflogT>::sandbox[] = "./cvmfs_ut_reflog";

template <class ReflogT>
const std::string T_Reflog<ReflogT>::fqrn = "test.cern.ch";

typedef ::testing::Types<manifest::Reflog, MockReflog> ReflogTypes;
TYPED_TEST_CASE(T_Reflog, ReflogTypes);


TYPED_TEST(T_Reflog, Initalize) {}


TYPED_TEST(T_Reflog, CreateReflog) {
  const std::string rp = TestFixture::GetReflogFilename();
  typedef typename TestFixture::Reflog Reflog;

  Reflog *reflog = TestFixture::CreateReflog(rp);
  ASSERT_NE(static_cast<Reflog*>(NULL), reflog);
  EXPECT_EQ(TestFixture::fqrn, reflog->fqrn());
  TestFixture::CloseReflog(reflog);
}


TYPED_TEST(T_Reflog, OpenReflog) {
  const std::string rp = TestFixture::GetReflogFilename();
  typedef typename TestFixture::Reflog Reflog;

  Reflog *rl1 = TestFixture::CreateReflog(rp);
  ASSERT_NE(static_cast<Reflog*>(NULL), rl1);
  EXPECT_EQ(TestFixture::fqrn, rl1->fqrn());
  TestFixture::CloseReflog(rl1);

  Reflog *rl2 = TestFixture::OpenReflog(rp);
  ASSERT_NE(static_cast<Reflog*>(NULL), rl2);
  EXPECT_EQ(TestFixture::fqrn, rl2->fqrn());
  TestFixture::CloseReflog(rl2);
}
