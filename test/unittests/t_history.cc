/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <map>
#include <string>

#include "../../cvmfs/compression.h"
#include "../../cvmfs/history_sqlite.h"
#include "../../cvmfs/prng.h"
#include "../../cvmfs/util.h"
#include "testutil.h"

using history::History;
using history::SqliteHistory;

template <class HistoryT>
class T_History : public ::testing::Test {
 protected:
  static const char sandbox[];
  static const std::string fqrn;

  static const std::string history_v1_r0;
  static const std::string history_v1_r1;

  static const std::string history_v1_r0_path;
  static const std::string history_v1_r1_path;

  typedef std::vector<History::Tag>            TagVector;
  typedef std::map<std::string, MockHistory*>  MockHistoryMap;

 protected:
  virtual void SetUp() {
    if (NeedsSandbox()) {
      ASSERT_TRUE(MkdirDeep(string(sandbox), 0700))
                  << "failed to create sandbox";
      PrepareLegacyHistoryDBs();
    }
    prng_.InitSeed(42);
  }

  virtual void TearDown() {
    if (NeedsSandbox()) {
      const bool retval = RemoveTree(string(sandbox));
      ASSERT_TRUE(retval) << "failed to remove sandbox";
    }

    // clear the mock history map
          MockHistoryMap::const_iterator i    = mock_history_map_.begin();
    const MockHistoryMap::const_iterator iend = mock_history_map_.end();
    for (; i != iend; ++i) {
      delete i->second;
    }
    mock_history_map_.clear();
  }

 private:
  // type-based overlaoded instantiation of History object wrapper
  // Inspired from here:
  //   http://stackoverflow.com/questions/5512910/
  //          explicit-specialization-of-template-class-member-function
  template <typename T> struct type {};

  History* CreateHistory(const type<history::SqliteHistory>  type_specifier,
                         const std::string                  &filename) {
    return SqliteHistory::Create(filename, fqrn);
  }

  History* CreateHistory(const type<MockHistory>  type_specifier,
                         const std::string       &filename) {
    const bool writable         = true;
    MockHistory *new_hist       = new MockHistory(writable, fqrn);
    mock_history_map_[filename] = new_hist;
    return new_hist;
  }

  History *OpenMockHistory(const std::string &filename, const bool writable) {
    const MockHistoryMap::const_iterator h = mock_history_map_.find(filename);
    if (h == mock_history_map_.end()) {
      return NULL;
    }

    MockHistory *history = h->second;
    history->set_writable(writable);
    return history;
  }

  History* OpenHistory(const type<history::SqliteHistory>  type_specifier,
                       const std::string                  &filename) {
    return SqliteHistory::Open(filename);
  }

  History* OpenHistory(const type<MockHistory>  type_specifier,
                       const std::string       &filename) {
    return OpenMockHistory(filename, false);
  }

  History* OpenWritableHistory(
    const type<history::SqliteHistory> type_specifier,
    const std::string &filename
  ) {
    return SqliteHistory::OpenWritable(filename);
  }

  History* OpenWritableHistory(const type<MockHistory>  type_specifier,
                               const std::string       &filename) {
    return OpenMockHistory(filename, false);
  }

  void CloseHistory(SqliteHistory *history) {
    delete history;
  }

  void CloseHistory(MockHistory *history) {
    // NOOP
  }

  bool NeedsSandbox(const type<history::SqliteHistory> type_specifier) const {
    return true;
  }

  bool NeedsSandbox(const type<MockHistory> type_specifier) const {
    return false;
  }

  bool IsMocked(const type<history::SqliteHistory> type_specifier) const {
    return false;
  }

  bool IsMocked(const type<MockHistory> type_specifier) const {
    return true;
  }

 protected:
  History* CreateHistory(const std::string &filename) {
    return CreateHistory(type<HistoryT>(), filename);
  }

  History* OpenHistory(const std::string &filename) {
    return OpenHistory(type<HistoryT>(), filename);
  }

  History* OpenWritableHistory(const std::string &filename) {
    return OpenWritableHistory(type<HistoryT>(), filename);
  }

  void CloseHistory(History* history) {
    CloseHistory(static_cast<HistoryT*>(history));
  }

  bool NeedsSandbox() const {
    return NeedsSandbox(type<HistoryT>());
  }

  bool IsMocked() const {
    return IsMocked(type<HistoryT>());
  }

  std::string GetHistoryFilename() const {
    std::string path;
    if (NeedsSandbox()) {
      path = CreateTempPath(string(sandbox) + "/history", 0600);
    } else {
      do {
        path = StringifyInt(prng_.Next(123652348));
      } while (mock_history_map_.find(path) != mock_history_map_.end());
    }
    CheckEmpty(path);
    return path;
  }

  History::Tag GetDummyTag(
        const std::string            &name      = "foobar",
        const uint64_t                revision  = 42,
        const History::UpdateChannel  channel   = History::kChannelTest,
        const time_t                  timestamp = 564993000) const {
      shash::Any root_hash(shash::kSha1);
      root_hash.Randomize();

      History::Tag dummy;
      dummy.name        = name;
      dummy.root_hash   = root_hash;
      dummy.size        = 1337;
      dummy.revision    = revision;
      dummy.timestamp   = timestamp;
      dummy.channel     = channel;
      dummy.description = "This is just a small dummy";

      return dummy;
  }

  TagVector GetDummyTags(const unsigned int count) const {
    TagVector result;
    result.reserve(count);
    for (unsigned int i = 0; i < count; ++i) {
      shash::Any root_hash(shash::kSha1);
      root_hash.Randomize(&prng_);

      History::Tag dummy;
      dummy.name        = "dummy" + StringifyInt(i);
      dummy.root_hash   = root_hash;
      dummy.size        = prng_.Next(1024);
      dummy.revision    = i;
      dummy.timestamp   = prng_.Next(564993000);
      dummy.channel     = History::kChannelDevel;
      dummy.description = "This is just a small dummy with number " +
                          StringifyInt(i);

      result.push_back(dummy);
    }

    return result;
  }

  void PrepareLegacyHistoryDBs() const {
    UnpackHistory(history_v1_r0, history_v1_r0_path);
    UnpackHistory(history_v1_r1, history_v1_r1_path);
  }

  void UnpackHistory(const std::string &base64, const std::string &dest) const {
    ASSERT_EQ(0u, base64.length() % 4);
    std::string  decoded;
    char        *unpacked;
    uint64_t     unpacked_size;
    ASSERT_TRUE(Debase64(base64, &decoded)) << "failed to decode base64";
    ASSERT_TRUE(zlib::DecompressMem2Mem(
                   decoded.data(),
                   decoded.size(),
                   reinterpret_cast<void**>(&unpacked),
                   &unpacked_size)) << "failed to decompress";
    WriteFile(dest, std::string(unpacked, unpacked_size));
    free(unpacked);
  }

  void WriteFile(const std::string &path, const std::string &content) const {
    FILE *f = fopen(path.c_str(), "w+");
    ASSERT_NE(static_cast<FILE*>(NULL), f)
      << "failed to open. errno: " << errno;
    const size_t bytes_written = fwrite(content.data(), 1, content.length(), f);
    ASSERT_EQ(bytes_written, content.length())
      << "failed to write. errno: " << errno;

    const int retval = fclose(f);
    ASSERT_EQ(0, retval) << "failed to close. errno: " << errno;
  }

  bool CheckListing(const TagVector &lhs, const TagVector &rhs) const {
    if (lhs.size() != rhs.size()) {
      return false;
    }

          TagVector::const_iterator i    = lhs.begin();
    const TagVector::const_iterator iend = lhs.end();
    for (; i != iend; ++i) {
      bool found = false;
            TagVector::const_iterator j    = rhs.begin();
      const TagVector::const_iterator jend = rhs.end();
      for (; j != jend; ++j) {
        if (TagsEqual(*i, *j)) {
          found = true;
          break;
        }
      }

      if (!found) {
        return false;
      }
    }

    return true;
  }

  bool TagsEqual(const History::Tag &lhs, const History::Tag &rhs) const {
    return (lhs.name        == rhs.name)        &&
           (lhs.root_hash   == rhs.root_hash)   &&
           (lhs.size        == rhs.size)        &&
           (lhs.revision    == rhs.revision)    &&
           (lhs.timestamp   == rhs.timestamp)   &&
           (lhs.channel     == rhs.channel)     &&
           (lhs.description == rhs.description);
  }

  void CompareTags(const History::Tag &lhs, const History::Tag &rhs) const {
    EXPECT_EQ(lhs.name,        rhs.name);
    EXPECT_EQ(lhs.root_hash,   rhs.root_hash);
    EXPECT_EQ(lhs.size,        rhs.size);
    EXPECT_EQ(lhs.revision,    rhs.revision);
    EXPECT_EQ(lhs.timestamp,   rhs.timestamp);
    EXPECT_EQ(lhs.channel,     rhs.channel);
    EXPECT_EQ(lhs.description, rhs.description);
  }

 private:
  void CheckEmpty(const std::string &str) const {
    ASSERT_FALSE(str.empty());
  }

 protected:
  mutable Prng    prng_;

 private:
  MockHistoryMap  mock_history_map_;
};

template <class HistoryT>
const char T_History<HistoryT>::sandbox[] = "./cvmfs_ut_history";

template <class HistoryT>
const std::string T_History<HistoryT>::fqrn    = "test.cern.ch";

template <class HistoryT>
const std::string T_History<HistoryT>::history_v1_r0_path =
  string(T_History<HistoryT>::sandbox) + "/history_v1_r0";

template <class HistoryT>
const std::string T_History<HistoryT>::history_v1_r1_path =
  string(T_History<HistoryT>::sandbox) + "/history_v1_r1";

template <class HistoryT>
const std::string T_History<HistoryT>::history_v1_r0 =
  "eJztlb1v00AUwM8+N00QnVBkul0main9cBxBUJeayqoiSgHHAx1QdLEv8Sn+qn2uqJjKH8HM/4SQ"
  "mJnpwgALA3dOkVMkBAsClfvp3tO7p3vn92T7vdHTQ8oImqZ5jBmygAYUBewhBABocFkBNSoXbWmv"
  "gF/TAJsfjDX4FWhqCmAHvlVT9eNvxEmuJy/hqt7pKOc2w5OIZHmakZxRUtSWtu86tucgz35w6KDa"
  "jzbm5Ax5zjOvi05xVJJLe//x0chz7eGRh7L5eOn8E3f4yHaP0UPnuIo1DEtr6AcdBdAkIC+Kk4h/"
  "+GNcsrTaL4WOzdpeOd9WVvV2W3m1SJnhWSFEvZKm8KCNBMffswpxEV6aOTmlBU0TxFN0Dhy3ixBi"
  "NCb8mjirnX6Ik4REtSMghZ/TjInQxU0/1Fo99EqVIgHDuKM29N32z8oUUWNTaMjfh7bGlQJfA74k"
  "Esk/Qch7zvOW1oxZXibzwLT8+4NJf9If+CTAfoCt3oAQ3MfWzqB31+KYPd+8N3ozfBdhxjsLyspJ"
  "RIuQBKhIcFaEKesi0QP4kKc+jqIzVGYBPxrcAOL//wL4kkgk/wdNqLeq3iLmvwrfA/iJK4lEci24"
  "rcL29vQkT/w0mdLZJpnRrUrKmwpcv1X4IYmxubUj5r8KLwC8gJ//ds4SieQP0oRtRfQEtQXXW4sW"
  "8A15t8pp";

template <class HistoryT>
const std::string T_History<HistoryT>::history_v1_r1 =
  "eJztl09r1EAUwCd5absVS6FlWXoQpniwoX/Y7KbZLaK4lqUUa9VtDvYg28lkYkKzSZqZLa2e6kfw"
  "W3n21EMvxU/gQS8ezOy2ZFcoehDEkh/M5M1j3j+SN0P2Xu0EgmEvTntE4DrSkKKgJxgjhGayMYFy"
  "1GxoI2sF/Z4ZtHqpz8APpKkHCObgs3qgXvyBXcEt4T1MVRYXlbOWIE7IkjROWCoCxnNJ2+y0W3Yb"
  "262nO22c6/HSITvFdvu1vYKPSdhnV/Lmi909u9Pa3rVxctgd2f+ys/281dnHz9r7A1tdr2uTla1F"
  "BQWRy074UZh96V3SF/FgPWLaNXJ54uyxMlUpl5UP3UHKgrzlcqhjaUoNXopI7zorn3D/SkzZccCD"
  "OMJZiu2tdmcFYyyCHsvc9JJcSX0SRSzMFS7jNA0SIU2Hnnjwjo16GS99kMNY0TIfXX+gTlYelm+q"
  "Wlp1DTlD9no02eQq3EPwMZsKCgpyuFrS3yhaKVBF2o8OVxPZ2nGfNxuWZXoNt05N0lgnTbrhWTVW"
  "J2bD2bBqtFo1XMtZrxHV5rN60nfCgPvMxRE7EVjEeOBsBcuezC7dgJIwPMX9xCWCuU0UKqWKDNob"
  "BvVMx3Isz2IbTeZQ5hGTsapBTcOxTIcQ5jhmbZ0yF2z+KAgzF1zgPCSPSML9WNwY7Q6S/f8Fwfds"
  "KigouHXMga6MH2BqCSrTA5W8/wE+IfgKF9mjoKDgf2QOoHzfO0ojj9I1ytJojfrzKizPcuqzHule"
  "/5UYdxVYmB8qjbWqvP8BzhGcwze4/NdFFBQU/BVKUFbkcQDzsKz8cgao07AwPdT9BBspCA8=";


typedef ::testing::Types<history::SqliteHistory, MockHistory> HistoryTypes;
TYPED_TEST_CASE(T_History, HistoryTypes);


TYPED_TEST(T_History, Initialize) {}


TYPED_TEST(T_History, CreateHistory) {
  const std::string hp = TestFixture::GetHistoryFilename();
  History *history = TestFixture::CreateHistory(hp);
  ASSERT_NE(static_cast<History*>(NULL), history);
  EXPECT_EQ(TestFixture::fqrn, history->fqrn());
  TestFixture::CloseHistory(history);
}


TYPED_TEST(T_History, OpenHistory) {
  const std::string hp = TestFixture::GetHistoryFilename();
  History *history1 = TestFixture::CreateHistory(hp);
  ASSERT_NE(static_cast<History*>(NULL), history1);
  EXPECT_EQ(TestFixture::fqrn, history1->fqrn());
  TestFixture::CloseHistory(history1);

  History *history2 = TestFixture::OpenHistory(hp);
  ASSERT_NE(static_cast<History*>(NULL), history2);
  EXPECT_EQ(TestFixture::fqrn, history2->fqrn());
  TestFixture::CloseHistory(history2);
}


TYPED_TEST(T_History, InsertTag) {
  const std::string hp = TestFixture::GetHistoryFilename();
  History *history = TestFixture::CreateHistory(hp);
  ASSERT_NE(static_cast<History*>(NULL), history);
  EXPECT_EQ(TestFixture::fqrn, history->fqrn());
  ASSERT_TRUE(history->Insert(TestFixture::GetDummyTag()));
  EXPECT_EQ(1u, history->GetNumberOfTags());
  TestFixture::CloseHistory(history);
}


TYPED_TEST(T_History, InsertTwice) {
  const std::string hp = TestFixture::GetHistoryFilename();
  History *history = TestFixture::CreateHistory(hp);
  ASSERT_NE(static_cast<History*>(NULL), history);
  EXPECT_EQ(TestFixture::fqrn, history->fqrn());
  ASSERT_TRUE(history->Insert(TestFixture::GetDummyTag()));
  EXPECT_EQ(1u, history->GetNumberOfTags());
  ASSERT_FALSE(history->Insert(TestFixture::GetDummyTag()));
  EXPECT_EQ(1u, history->GetNumberOfTags());
  TestFixture::CloseHistory(history);
}


TYPED_TEST(T_History, CountTags) {
  typedef typename TestFixture::TagVector    TagVector;
  typedef typename TagVector::const_iterator TagVectorItr;

  const std::string hp = TestFixture::GetHistoryFilename();
  History *history = TestFixture::CreateHistory(hp);
  ASSERT_NE(static_cast<History*>(NULL), history);
  EXPECT_EQ(TestFixture::fqrn, history->fqrn());

  const unsigned int dummy_count = 1000;
  const TagVector dummy_tags = TestFixture::GetDummyTags(dummy_count);
  ASSERT_TRUE(history->BeginTransaction());
        TagVectorItr i    = dummy_tags.begin();
  const TagVectorItr iend = dummy_tags.end();
  for (; i != iend; ++i) {
    ASSERT_TRUE(history->Insert(*i));
  }
  EXPECT_TRUE(history->CommitTransaction());

  EXPECT_EQ(dummy_count, history->GetNumberOfTags());

  TestFixture::CloseHistory(history);
}


TYPED_TEST(T_History, InsertAndFindTag) {
  const std::string hp = TestFixture::GetHistoryFilename();
  History *history = TestFixture::CreateHistory(hp);
  ASSERT_NE(static_cast<History*>(NULL), history);
  EXPECT_EQ(TestFixture::fqrn, history->fqrn());
  History::Tag dummy = TestFixture::GetDummyTag();
  EXPECT_TRUE(history->Insert(dummy));
  EXPECT_EQ(1u, history->GetNumberOfTags());

  History::Tag tag;
  ASSERT_TRUE(history->GetByName(dummy.name, &tag));
  TestFixture::CompareTags(dummy, tag);

  TestFixture::CloseHistory(history);
}


TYPED_TEST(T_History, InsertReopenAndFindTag) {
  const std::string hp = TestFixture::GetHistoryFilename();
  History *history1 = TestFixture::CreateHistory(hp);
  ASSERT_NE(static_cast<History*>(NULL), history1);
  EXPECT_EQ(TestFixture::fqrn, history1->fqrn());
  History::Tag dummy = TestFixture::GetDummyTag();
  EXPECT_TRUE(history1->Insert(dummy));
  EXPECT_EQ(1u, history1->GetNumberOfTags());

  History::Tag tag1;
  ASSERT_TRUE(history1->GetByName(dummy.name, &tag1));
  TestFixture::CompareTags(dummy, tag1);
  TestFixture::CloseHistory(history1);

  History *history2 = TestFixture::OpenHistory(hp);
  ASSERT_NE(static_cast<History*>(NULL), history2);
  EXPECT_EQ(TestFixture::fqrn, history2->fqrn());
  EXPECT_EQ(1u, history2->GetNumberOfTags());

  History::Tag tag2;
  ASSERT_TRUE(history2->GetByName(dummy.name, &tag2));
  TestFixture::CompareTags(dummy, tag2);
  TestFixture::CloseHistory(history2);
}


TYPED_TEST(T_History, ListTags) {
  typedef typename TestFixture::TagVector            TagVector;
  typedef typename TagVector::const_iterator         TagVectorItr;
  typedef typename TagVector::const_reverse_iterator TagVectorRevItr;

  const std::string hp = TestFixture::GetHistoryFilename();
  History *history = TestFixture::CreateHistory(hp);
  ASSERT_NE(static_cast<History*>(NULL), history);
  EXPECT_EQ(TestFixture::fqrn, history->fqrn());

  const unsigned int dummy_count = 1000;
  const TagVector dummy_tags = TestFixture::GetDummyTags(dummy_count);
  ASSERT_TRUE(history->BeginTransaction());
        TagVectorItr i    = dummy_tags.begin();
  const TagVectorItr iend = dummy_tags.end();
  for (; i != iend; ++i) {
    ASSERT_TRUE(history->Insert(*i));
  }
  EXPECT_TRUE(history->CommitTransaction());

  EXPECT_EQ(dummy_count, history->GetNumberOfTags());

  TagVector tags;
  ASSERT_TRUE(history->List(&tags));
  EXPECT_EQ(dummy_count, tags.size());

                        i    = dummy_tags.begin();
        TagVectorRevItr j    = tags.rbegin();
  const TagVectorRevItr jend = tags.rend();
  for (; j != jend; ++j, ++i) {
    TestFixture::CompareTags(*i, *j);
  }

  TestFixture::CloseHistory(history);
}


TYPED_TEST(T_History, InsertAndRemoveTag) {
  typedef typename TestFixture::TagVector            TagVector;
  typedef typename TagVector::const_iterator         TagVectorItr;
  typedef typename TagVector::const_reverse_iterator TagVectorRevItr;

  const std::string hp = TestFixture::GetHistoryFilename();
  History *history = TestFixture::CreateHistory(hp);
  ASSERT_NE(static_cast<History*>(NULL), history);
  EXPECT_EQ(TestFixture::fqrn, history->fqrn());

  const unsigned int dummy_count = 40;
  const TagVector dummy_tags = TestFixture::GetDummyTags(dummy_count);
  ASSERT_TRUE(history->BeginTransaction());
        TagVectorItr i    = dummy_tags.begin();
  const TagVectorItr iend = dummy_tags.end();
  for (; i != iend; ++i) {
    ASSERT_TRUE(history->Insert(*i));
  }
  EXPECT_TRUE(history->CommitTransaction());
  EXPECT_EQ(dummy_count, history->GetNumberOfTags());

  const std::string to_be_deleted = dummy_tags[5].name;
  EXPECT_TRUE(history->Exists(to_be_deleted));
  ASSERT_TRUE(history->Remove(dummy_tags[5].name));
  EXPECT_EQ(dummy_count - 1, history->GetNumberOfTags());
  EXPECT_FALSE(history->Exists(to_be_deleted));

  TagVector tags;
  ASSERT_TRUE(history->List(&tags));
  EXPECT_EQ(dummy_count - 1, tags.size());

                        i    = dummy_tags.begin();
        TagVectorRevItr j    = tags.rbegin();
  const TagVectorRevItr jend = tags.rend();
  for (; j != jend; ++j, ++i) {
    if (i->name == to_be_deleted) {
      --j;
      continue;
    }
    TestFixture::CompareTags(*i, *j);
  }

  TestFixture::CloseHistory(history);
}


TYPED_TEST(T_History, RemoveNonExistentTag) {
  typedef typename TestFixture::TagVector            TagVector;
  typedef typename TagVector::const_iterator         TagVectorItr;
  typedef typename TagVector::const_reverse_iterator TagVectorRevItr;

  const std::string hp = TestFixture::GetHistoryFilename();
  History *history = TestFixture::CreateHistory(hp);
  ASSERT_NE(static_cast<History*>(NULL), history);
  EXPECT_EQ(TestFixture::fqrn, history->fqrn());

  const unsigned int dummy_count = 40;
  const TagVector dummy_tags = TestFixture::GetDummyTags(dummy_count);
  ASSERT_TRUE(history->BeginTransaction());
        TagVectorItr i    = dummy_tags.begin();
  const TagVectorItr iend = dummy_tags.end();
  for (; i != iend; ++i) {
    ASSERT_TRUE(history->Insert(*i));
  }
  EXPECT_TRUE(history->CommitTransaction());
  EXPECT_EQ(dummy_count, history->GetNumberOfTags());

  ASSERT_TRUE(history->Remove("doesnt_exist"));
  EXPECT_EQ(dummy_count, history->GetNumberOfTags());

  TagVector tags;
  ASSERT_TRUE(history->List(&tags));
  EXPECT_EQ(dummy_count, tags.size());

                        i    = dummy_tags.begin();
        TagVectorRevItr j    = tags.rbegin();
  const TagVectorRevItr jend = tags.rend();
  for (; j != jend; ++j, ++i) {
    TestFixture::CompareTags(*i, *j);
  }

  TestFixture::CloseHistory(history);
}


TYPED_TEST(T_History, RemoveMultipleTagsSlow) {
  typedef typename TestFixture::TagVector            TagVector;
  typedef typename TagVector::const_iterator         TagVectorItr;
  typedef typename TagVector::const_reverse_iterator TagVectorRevItr;

  const std::string hp = TestFixture::GetHistoryFilename();
  History *history = TestFixture::CreateHistory(hp);
  ASSERT_NE(static_cast<History*>(NULL), history);
  EXPECT_EQ(TestFixture::fqrn, history->fqrn());

  const unsigned int dummy_count = 40;
  const TagVector dummy_tags = TestFixture::GetDummyTags(dummy_count);
  ASSERT_TRUE(history->BeginTransaction());
        TagVectorItr i    = dummy_tags.begin();
  const TagVectorItr iend = dummy_tags.end();
  for (; i != iend; ++i) {
    ASSERT_TRUE(history->Insert(*i));
  }
  EXPECT_TRUE(history->CommitTransaction());
  EXPECT_EQ(dummy_count, history->GetNumberOfTags());

  std::vector<std::string> to_be_deleted;
  to_be_deleted.push_back(dummy_tags[2].name);
  to_be_deleted.push_back(dummy_tags[5].name);
  to_be_deleted.push_back(dummy_tags[10].name);
  to_be_deleted.push_back(dummy_tags[15].name);

        std::vector<std::string>::const_iterator j    = to_be_deleted.begin();
  const std::vector<std::string>::const_iterator jend = to_be_deleted.end();
  for (; j != jend; ++j) {
    EXPECT_TRUE(history->Remove(*j));
  }
  EXPECT_EQ(dummy_count - to_be_deleted.size(), history->GetNumberOfTags());

  TagVector tags;
  ASSERT_TRUE(history->List(&tags));
  EXPECT_EQ(dummy_count - to_be_deleted.size(), tags.size());

                        i    = dummy_tags.begin();
        TagVectorRevItr k    = tags.rbegin();
  const TagVectorRevItr kend = tags.rend();
  for (; k != kend; ++k, ++i) {
    bool should_exist = true;
          std::vector<std::string>::const_iterator l    = to_be_deleted.begin();
    const std::vector<std::string>::const_iterator lend = to_be_deleted.end();
    for (; l != lend; ++l) {
      if (i->name == *l) {
        --k;
        should_exist = false;
        break;
      }
    }
    if (should_exist) {
      TestFixture::CompareTags(*i, *k);
    }
  }

  TestFixture::CloseHistory(history);
}


TYPED_TEST(T_History, RemoveTagsWithReOpen) {
  typedef typename TestFixture::TagVector            TagVector;
  typedef typename TagVector::const_iterator         TagVectorItr;
  typedef typename TagVector::const_reverse_iterator TagVectorRevItr;

  const std::string hp = TestFixture::GetHistoryFilename();
  History *history1 = TestFixture::CreateHistory(hp);
  ASSERT_NE(static_cast<History*>(NULL), history1);
  EXPECT_EQ(TestFixture::fqrn, history1->fqrn());

  const unsigned int dummy_count = 40;
  const TagVector dummy_tags = TestFixture::GetDummyTags(dummy_count);
  ASSERT_TRUE(history1->BeginTransaction());
        TagVectorItr i    = dummy_tags.begin();
  const TagVectorItr iend = dummy_tags.end();
  for (; i != iend; ++i) {
    ASSERT_TRUE(history1->Insert(*i));
  }
  EXPECT_TRUE(history1->CommitTransaction());
  EXPECT_EQ(dummy_count, history1->GetNumberOfTags());
  TestFixture::CloseHistory(history1);

  History *history2 = TestFixture::OpenWritableHistory(hp);
  ASSERT_NE(static_cast<History*>(NULL), history2);
  EXPECT_EQ(TestFixture::fqrn, history2->fqrn());

  std::vector<std::string> to_be_deleted;
  to_be_deleted.push_back(dummy_tags[2].name);
  to_be_deleted.push_back(dummy_tags[5].name);
  to_be_deleted.push_back(dummy_tags[10].name);
  to_be_deleted.push_back(dummy_tags[15].name);

        std::vector<std::string>::const_iterator j    = to_be_deleted.begin();
  const std::vector<std::string>::const_iterator jend = to_be_deleted.end();
  for (; j != jend; ++j) {
    EXPECT_TRUE(history2->Remove(*j));
  }
  EXPECT_EQ(dummy_count - to_be_deleted.size(), history2->GetNumberOfTags());
  TestFixture::CloseHistory(history2);

  History *history3 = TestFixture::OpenHistory(hp);
  TagVector tags;
  ASSERT_TRUE(history3->List(&tags));
  EXPECT_EQ(dummy_count - to_be_deleted.size(), tags.size());

                        i    = dummy_tags.begin();
        TagVectorRevItr k    = tags.rbegin();
  const TagVectorRevItr kend = tags.rend();
  for (; k != kend; ++k, ++i) {
    bool should_exist = true;
          std::vector<std::string>::const_iterator l    = to_be_deleted.begin();
    const std::vector<std::string>::const_iterator lend = to_be_deleted.end();
    for (; l != lend; ++l) {
      if (i->name == *l) {
        --k;
        should_exist = false;
        break;
      }
    }
    if (should_exist) {
      TestFixture::CompareTags(*i, *k);
    }
  }

  TestFixture::CloseHistory(history3);
}


TYPED_TEST(T_History, GetChannelTips) {
  typedef typename TestFixture::TagVector TagVector;
  typedef TestFixture                     TF;

  const std::string hp = TestFixture::GetHistoryFilename();
  History *history1 = TestFixture::CreateHistory(hp);
  ASSERT_NE(static_cast<History*>(NULL), history1);
  EXPECT_EQ(TestFixture::fqrn, history1->fqrn());

  history1->BeginTransaction();
  const History::Tag trunk_tip =
    TF::GetDummyTag("zap", 4, History::kChannelTrunk);
  ASSERT_TRUE(history1->Insert(
    TF::GetDummyTag("foo", 1, History::kChannelTrunk)));
  ASSERT_TRUE(history1->Insert(
    TF::GetDummyTag("bar", 2, History::kChannelTrunk)));
  ASSERT_TRUE(history1->Insert(
    TF::GetDummyTag("baz", 3, History::kChannelTrunk)));
  ASSERT_TRUE(history1->Insert(trunk_tip));

  const History::Tag test_tip =
    TF::GetDummyTag("yolo",   6, History::kChannelTest);
  ASSERT_TRUE(history1->Insert(
    TF::GetDummyTag("moep", 3, History::kChannelTest)));
  ASSERT_TRUE(history1->Insert(
    TF::GetDummyTag("lol", 4, History::kChannelTest)));
  ASSERT_TRUE(history1->Insert(
    TF::GetDummyTag("cheers", 5, History::kChannelTest)));
  ASSERT_TRUE(history1->Insert(test_tip));
  history1->CommitTransaction();

  TagVector tags;
  ASSERT_TRUE(history1->Tips(&tags));
  EXPECT_EQ(2u, tags.size());

  TagVector expected;  // TODO(rmeusel): C++11 initializer lists
  expected.push_back(trunk_tip);
  expected.push_back(test_tip);
  EXPECT_TRUE(TestFixture::CheckListing(tags, expected));

  history1->BeginTransaction();
  const History::Tag prod_tip =
    TF::GetDummyTag("prod", 10, History::kChannelProd);
  ASSERT_TRUE(history1->Insert(
    TF::GetDummyTag("vers", 3, History::kChannelProd)));
  ASSERT_TRUE(history1->Insert(
    TF::GetDummyTag("bug",  6, History::kChannelProd)));
  ASSERT_TRUE(history1->Insert(prod_tip));
  history1->CommitTransaction();

  tags.clear();
  ASSERT_TRUE(history1->Tips(&tags));
  EXPECT_EQ(3u, tags.size());

  expected.push_back(prod_tip);
  EXPECT_TRUE(TestFixture::CheckListing(tags, expected));

  TestFixture::CloseHistory(history1);

  History *history2 = TestFixture::OpenHistory(hp);
  ASSERT_NE(static_cast<History*>(NULL), history2);
  EXPECT_EQ(TestFixture::fqrn, history2->fqrn());

  tags.clear();
  ASSERT_TRUE(history2->Tips(&tags));
  EXPECT_EQ(3u, tags.size());
  EXPECT_TRUE(TestFixture::CheckListing(tags, expected));

  TestFixture::CloseHistory(history2);
}


TYPED_TEST(T_History, GetHashes) {
  typedef typename TestFixture::TagVector            TagVector;
  typedef typename TagVector::const_iterator         TagVectorItr;
  typedef typename TagVector::const_reverse_iterator TagVectorRevItr;

  const std::string hp = TestFixture::GetHistoryFilename();
  History *history = TestFixture::CreateHistory(hp);
  ASSERT_NE(static_cast<History*>(NULL), history);
  EXPECT_EQ(TestFixture::fqrn, history->fqrn());

  const unsigned int dummy_count = 1000;
  const TagVector dummy_tags = TestFixture::GetDummyTags(dummy_count);
  ASSERT_TRUE(history->BeginTransaction());
        TagVectorRevItr i    = dummy_tags.rbegin();
  const TagVectorRevItr iend = dummy_tags.rend();
  for (; i != iend; ++i) {
    ASSERT_TRUE(history->Insert(*i));
  }
  EXPECT_TRUE(history->CommitTransaction());

  EXPECT_EQ(dummy_count, history->GetNumberOfTags());

  std::vector<shash::Any> hashes;
  ASSERT_TRUE(history->GetHashes(&hashes));

        TagVectorItr j    = dummy_tags.begin();
  const TagVectorItr jend = dummy_tags.end();
        std::vector<shash::Any>::const_iterator k    = hashes.begin();
  ASSERT_EQ(dummy_tags.size(), hashes.size());
  for (; j != jend; ++j, ++k) {
    EXPECT_EQ(j->root_hash, *k);
  }

  TestFixture::CloseHistory(history);
}


TYPED_TEST(T_History, GetHashesWithDuplicates) {
  typedef typename TestFixture::TagVector            TagVector;
  typedef typename TagVector::const_iterator         TagVectorItr;
  typedef typename TagVector::const_reverse_iterator TagVectorRevItr;

  const std::string hp = TestFixture::GetHistoryFilename();
  History *history = TestFixture::CreateHistory(hp);
  ASSERT_NE(static_cast<History*>(NULL), history);
  EXPECT_EQ(TestFixture::fqrn, history->fqrn());

  const unsigned int dummy_count         = 1000;
  const unsigned int duplicate_frequency = 40;
  const TagVector dummy_tags = TestFixture::GetDummyTags(dummy_count);
  shash::Any dupl_hash(shash::kSha1);
  dupl_hash.Randomize(&this->prng_);
  ASSERT_TRUE(history->BeginTransaction());
        TagVectorRevItr i    = dummy_tags.rbegin();
  const TagVectorRevItr iend = dummy_tags.rend();
  for (unsigned int nr = 0; i != iend; ++i, ++nr) {
    ASSERT_TRUE(history->Insert(*i));
    if (nr % duplicate_frequency == 0) {
      // insert a duplicate
      History::Tag dupl = *i;
      dupl.name = i->name + "_duplicate";
      history->Insert(dupl);
    }
  }

  // insert three duplicates
  History::Tag dummy = TestFixture::GetDummyTag("dupl1", 2000);
  dummy.root_hash = dupl_hash;
  ASSERT_TRUE(history->Insert(dummy));

  dummy.name     = "dupl2";
  dummy.revision = 2001;
  ASSERT_TRUE(history->Insert(dummy));

  dummy.name     = "dupl3";
  dummy.revision = 2002;
  ASSERT_TRUE(history->Insert(dummy));

  EXPECT_TRUE(history->CommitTransaction());

  EXPECT_EQ(dummy_count + 3 + (dummy_count / duplicate_frequency),
             history->GetNumberOfTags());

  std::vector<shash::Any> hashes;
  ASSERT_TRUE(history->GetHashes(&hashes));

        TagVectorItr j    = dummy_tags.begin();
  const TagVectorItr jend = dummy_tags.end();
        std::vector<shash::Any>::const_iterator k    = hashes.begin();
  ASSERT_EQ(dummy_count + 1, hashes.size());
  for (; j != jend; ++j, ++k) {
    EXPECT_EQ(j->root_hash, *k);
  }

  // check the duplicate hash
  EXPECT_EQ(dupl_hash, *k);

  TestFixture::CloseHistory(history);
}


TYPED_TEST(T_History, GetTagByDate) {
  const std::string hp = TestFixture::GetHistoryFilename();
  History *history = TestFixture::CreateHistory(hp);
  ASSERT_NE(static_cast<History*>(NULL), history);
  EXPECT_EQ(TestFixture::fqrn, history->fqrn());

  const History::UpdateChannel c = History::kChannelTest;
  const History::Tag t3010 = TestFixture::GetDummyTag("f5", 1, c, 1414690911);
  const History::Tag t3110 = TestFixture::GetDummyTag("f4", 2, c, 1414777311);
  const History::Tag t0111 = TestFixture::GetDummyTag("f3", 3, c, 1414863711);
  const History::Tag t0211 = TestFixture::GetDummyTag("f2", 4, c, 1414950111);
  const History::Tag t0311 = TestFixture::GetDummyTag("f1", 5, c, 1415036511);

  history->BeginTransaction();
  ASSERT_TRUE(history->Insert(t0311));
  ASSERT_TRUE(history->Insert(t0211));
  ASSERT_TRUE(history->Insert(t0111));
  ASSERT_TRUE(history->Insert(t3110));
  ASSERT_TRUE(history->Insert(t3010));
  history->CommitTransaction();

  const time_t ts2510 = 1414255311;
  const time_t ts0111 = 1414864111;
  const time_t ts3110 = 1414777311;
  const time_t ts0411 = 1415126511;

  History::Tag tag;
  EXPECT_FALSE(history->GetByDate(ts2510, &tag));  // No revision yet

  EXPECT_TRUE(history->GetByDate(ts3110, &tag));
  TestFixture::CompareTags(t3110, tag);

  EXPECT_TRUE(history->GetByDate(ts0111, &tag));
  TestFixture::CompareTags(t0111, tag);

  EXPECT_TRUE(history->GetByDate(ts0411, &tag));
  TestFixture::CompareTags(t0311, tag);

  TestFixture::CloseHistory(history);
}


TYPED_TEST(T_History, RollbackToOldTag) {
  typedef typename TestFixture::TagVector TagVector;
  typedef TestFixture                     TF;

  const std::string hp = TestFixture::GetHistoryFilename();
  History *history1 = TestFixture::CreateHistory(hp);
  ASSERT_NE(static_cast<History*>(NULL), history1);
  EXPECT_EQ(TestFixture::fqrn, history1->fqrn());

  const History::UpdateChannel c_test = History::kChannelTest;
  const History::UpdateChannel c_prod = History::kChannelProd;

  ASSERT_TRUE(history1->BeginTransaction());
  ASSERT_TRUE(history1->Insert(TF::GetDummyTag("foo",            1, c_test)));
  ASSERT_TRUE(history1->Insert(TF::GetDummyTag("bar",            2, c_test)));
  ASSERT_TRUE(history1->Insert(TF::GetDummyTag("first_release",  3, c_prod)));
  ASSERT_TRUE(history1->Insert(TF::GetDummyTag("moep",           4, c_test)));
  ASSERT_TRUE(history1->Insert(TF::GetDummyTag("moep_duplicate", 4, c_test)));
  ASSERT_TRUE(history1->Insert(TF::GetDummyTag("lol",            5, c_test)));
  ASSERT_TRUE(history1->Insert(TF::GetDummyTag("second_release", 6, c_prod)));
  ASSERT_TRUE(history1->Insert(TF::GetDummyTag("third_release",  7, c_prod)));
  ASSERT_TRUE(history1->Insert(TF::GetDummyTag("rofl",           8, c_test)));
  ASSERT_TRUE(history1->Insert(TF::GetDummyTag("also_rofl",      8, c_test)));
  ASSERT_TRUE(history1->Insert(TF::GetDummyTag("forth_release",  9, c_prod)));
  ASSERT_TRUE(history1->CommitTransaction());

  TestFixture::CloseHistory(history1);

  History *history2 = TestFixture::OpenWritableHistory(hp);
  ASSERT_NE(static_cast<History*>(NULL), history2);
  EXPECT_EQ(TestFixture::fqrn, history2->fqrn());

  ASSERT_TRUE(history2->BeginTransaction());
  History::Tag rollback_target;
  EXPECT_TRUE(history2->GetByName("moep", &rollback_target));

  TagVector gone;
  EXPECT_TRUE(history2->ListTagsAffectedByRollback("moep", &gone));
  ASSERT_EQ(4u, gone.size());
  if (gone[0].name == "also_rofl") {  // order of rev 8 tags is undefined
    EXPECT_EQ("also_rofl", gone[0].name); EXPECT_EQ(8u, gone[0].revision);
    EXPECT_EQ("rofl",      gone[1].name); EXPECT_EQ(8u, gone[1].revision);
  } else {
    EXPECT_EQ("rofl",      gone[0].name); EXPECT_EQ(8u, gone[0].revision);
    EXPECT_EQ("also_rofl", gone[1].name); EXPECT_EQ(8u, gone[1].revision);
  }
  EXPECT_EQ("lol",       gone[2].name); EXPECT_EQ(5u, gone[2].revision);
  EXPECT_EQ("moep",      gone[3].name); EXPECT_EQ(4u, gone[3].revision);

  shash::Any new_root_hash(shash::kSha1);
  new_root_hash.Randomize();
  rollback_target.revision  = 10;
  rollback_target.root_hash = new_root_hash;

  EXPECT_TRUE(history2->Rollback(rollback_target));
  ASSERT_TRUE(history2->CommitTransaction());

  EXPECT_TRUE(history2->Exists("foo"));
  EXPECT_TRUE(history2->Exists("bar"));
  EXPECT_TRUE(history2->Exists("first_release"));
  EXPECT_TRUE(history2->Exists("moep"));
  EXPECT_TRUE(history2->Exists("moep_duplicate"));
  EXPECT_TRUE(history2->Exists("second_release"));
  EXPECT_TRUE(history2->Exists("third_release"));
  EXPECT_TRUE(history2->Exists("forth_release"));
  EXPECT_FALSE(history2->Exists("lol"));
  EXPECT_FALSE(history2->Exists("rofl"));
  EXPECT_FALSE(history2->Exists("also_rofl"));

  History::Tag rolled_back_tag;
  ASSERT_TRUE(history2->GetByName("moep", &rolled_back_tag));
  EXPECT_EQ(10u,           rolled_back_tag.revision);
  EXPECT_EQ(new_root_hash, rolled_back_tag.root_hash);

  TestFixture::CloseHistory(history2);

  History *history3 = TestFixture::OpenWritableHistory(hp);
  ASSERT_NE(static_cast<History*>(NULL), history3);
  EXPECT_EQ(TestFixture::fqrn, history3->fqrn());

  ASSERT_TRUE(history3->BeginTransaction());
  History::Tag rollback_target_malicious;
  EXPECT_TRUE(history3->GetByName("bar", &rollback_target_malicious));

  rollback_target_malicious.name      = "barlol";
  rollback_target_malicious.revision  = 11;
  EXPECT_FALSE(history3->Rollback(rollback_target_malicious));
  ASSERT_TRUE(history3->CommitTransaction());

  EXPECT_TRUE(history3->Exists("foo"));
  EXPECT_TRUE(history3->Exists("bar"));
  EXPECT_TRUE(history3->Exists("first_release"));
  EXPECT_TRUE(history3->Exists("moep"));
  EXPECT_TRUE(history3->Exists("moep_duplicate"));
  EXPECT_TRUE(history3->Exists("second_release"));
  EXPECT_TRUE(history3->Exists("third_release"));
  EXPECT_TRUE(history3->Exists("forth_release"));
  EXPECT_FALSE(history3->Exists("lol"));
  EXPECT_FALSE(history3->Exists("rofl"));
  EXPECT_FALSE(history3->Exists("also_rofl"));

  TestFixture::CloseHistory(history3);
}


TYPED_TEST(T_History, ListTagsAffectedByRollback) {
  typedef typename TestFixture::TagVector TagVector;
  typedef TestFixture                     TF;

  const std::string hp = TestFixture::GetHistoryFilename();
  History *history1 = TestFixture::CreateHistory(hp);
  ASSERT_NE(static_cast<History*>(NULL), history1);
  EXPECT_EQ(TestFixture::fqrn, history1->fqrn());

  const History::UpdateChannel c_test = History::kChannelTest;
  const History::UpdateChannel c_prod = History::kChannelProd;

  ASSERT_TRUE(history1->BeginTransaction());
  ASSERT_TRUE(history1->Insert(TF::GetDummyTag("foo",            1, c_test)));
  ASSERT_TRUE(history1->Insert(TF::GetDummyTag("bar",            2, c_test)));
  ASSERT_TRUE(history1->Insert(TF::GetDummyTag("first_release",  3, c_prod)));
  ASSERT_TRUE(history1->Insert(TF::GetDummyTag("test_release",   3, c_test)));
  ASSERT_TRUE(history1->Insert(TF::GetDummyTag("moep",           4, c_test)));
  ASSERT_TRUE(history1->Insert(TF::GetDummyTag("moep_duplicate", 4, c_test)));
  ASSERT_TRUE(history1->Insert(TF::GetDummyTag("lol",            5, c_test)));
  ASSERT_TRUE(history1->Insert(TF::GetDummyTag("second_release", 6, c_prod)));
  ASSERT_TRUE(history1->Insert(TF::GetDummyTag("third_release",  7, c_prod)));
  ASSERT_TRUE(history1->Insert(TF::GetDummyTag("rofl",           8, c_test)));
  ASSERT_TRUE(history1->Insert(TF::GetDummyTag("also_rofl",      8, c_test)));
  ASSERT_TRUE(history1->Insert(TF::GetDummyTag("forth_release",  9, c_prod)));
  ASSERT_TRUE(history1->CommitTransaction());

  TagVector gone;
  EXPECT_TRUE(history1->ListTagsAffectedByRollback("moep",  &gone));
  ASSERT_EQ(4u, gone.size());
  if (gone[0].name == "also_rofl") {  // order of rev 8 tags is undefined
    EXPECT_EQ("also_rofl", gone[0].name); EXPECT_EQ(8u, gone[0].revision);
    EXPECT_EQ("rofl",      gone[1].name); EXPECT_EQ(8u, gone[1].revision);
  } else {
    EXPECT_EQ("rofl",      gone[0].name); EXPECT_EQ(8u, gone[0].revision);
    EXPECT_EQ("also_rofl", gone[1].name); EXPECT_EQ(8u, gone[1].revision);
  }
  EXPECT_EQ("lol",       gone[2].name); EXPECT_EQ(5u, gone[2].revision);
  EXPECT_EQ("moep",      gone[3].name); EXPECT_EQ(4u, gone[3].revision);

  gone.clear();
  EXPECT_FALSE(history1->ListTagsAffectedByRollback("unobtainium", &gone));
  EXPECT_TRUE(gone.empty());

  gone.clear();
  EXPECT_TRUE(history1->ListTagsAffectedByRollback("second_release", &gone));
  ASSERT_EQ(3u, gone.size());
  EXPECT_EQ("forth_release",  gone[0].name); EXPECT_EQ(9u, gone[0].revision);
  EXPECT_EQ("third_release",  gone[1].name); EXPECT_EQ(7u, gone[1].revision);
  EXPECT_EQ("second_release", gone[2].name); EXPECT_EQ(6u, gone[2].revision);

  gone.clear();
  EXPECT_TRUE(history1->ListTagsAffectedByRollback("bar", &gone));
  ASSERT_EQ(7u, gone.size());
  if (gone[0].name == "also_rofl") {  // undefined order for same revision
    EXPECT_EQ("also_rofl",      gone[0].name); EXPECT_EQ(8u, gone[0].revision);
    EXPECT_EQ("rofl",           gone[1].name); EXPECT_EQ(8u, gone[1].revision);
  } else {
    EXPECT_EQ("rofl",           gone[0].name); EXPECT_EQ(8u, gone[0].revision);
    EXPECT_EQ("also_rofl",      gone[1].name); EXPECT_EQ(8u, gone[1].revision);
  }
  EXPECT_EQ("lol",            gone[2].name); EXPECT_EQ(5u, gone[2].revision);
  if (gone[3].name == "moep_duplicate") {  // undefined order of same revision
    EXPECT_EQ("moep_duplicate", gone[3].name); EXPECT_EQ(4u, gone[3].revision);
    EXPECT_EQ("moep",           gone[4].name); EXPECT_EQ(4u, gone[4].revision);
  } else {
    EXPECT_EQ("moep",           gone[3].name); EXPECT_EQ(4u, gone[3].revision);
    EXPECT_EQ("moep_duplicate", gone[4].name); EXPECT_EQ(4u, gone[4].revision);
  }
  EXPECT_EQ("test_release",   gone[5].name); EXPECT_EQ(3u, gone[5].revision);
  EXPECT_EQ("bar",            gone[6].name); EXPECT_EQ(2u, gone[6].revision);

  gone.clear();
  EXPECT_TRUE(history1->ListTagsAffectedByRollback("forth_release", &gone));
  ASSERT_EQ(1u, gone.size());
  EXPECT_EQ("forth_release", gone[0].name); EXPECT_EQ(9u, gone[0].revision);

  TestFixture::CloseHistory(history1);
}


TYPED_TEST(T_History, RecycleBinForRemovedTags) {
  const std::string hp = TestFixture::GetHistoryFilename();
  History *history1 = TestFixture::CreateHistory(hp);
  ASSERT_NE(static_cast<History*>(NULL), history1);
  EXPECT_EQ(TestFixture::fqrn, history1->fqrn());

  ASSERT_TRUE(history1->BeginTransaction());
  History::Tag dummy_foo;
  dummy_foo.name      = "foo";
  dummy_foo.root_hash =
    shash::MkFromHexPtr(
      shash::HexPtr("5207a527a4fee2d655c67415aa1979f1d2753f96"),
      shash::kSuffixCatalog);
  dummy_foo.revision  = 1;
  dummy_foo.channel   = History::kChannelTest;
  ASSERT_TRUE(history1->Insert(dummy_foo));

  History::Tag dummy_bar;
  dummy_bar.name      = "bar";
  dummy_bar.root_hash =
    shash::MkFromHexPtr(
      shash::HexPtr("19552496e1e5c63aefaf5d4e05a8c248a1d82663"),
      shash::kSuffixCatalog);
  dummy_bar.revision  = 2;
  dummy_bar.channel   = History::kChannelProd;
  ASSERT_TRUE(history1->Insert(dummy_bar));

  History::Tag dummy_baz;
  dummy_baz.name      = "baz";
  dummy_baz.root_hash =
    shash::MkFromHexPtr(
      shash::HexPtr("400b66c2002e89629dd098918677e818e3688011"),
      shash::kSuffixCatalog);
  dummy_baz.revision  = 3;
  dummy_baz.channel   = History::kChannelTest;
  ASSERT_TRUE(history1->Insert(dummy_baz));
  EXPECT_TRUE(history1->CommitTransaction());

  EXPECT_EQ(3u, history1->GetNumberOfTags());

  std::vector<shash::Any> hashes;
  ASSERT_TRUE(history1->ListRecycleBin(&hashes));
  EXPECT_EQ(0u, hashes.size());
  hashes.clear();

  TestFixture::CloseHistory(history1);

  History *history2 = TestFixture::OpenHistory(hp);
  EXPECT_EQ(3u, history2->GetNumberOfTags());

  ASSERT_TRUE(history2->ListRecycleBin(&hashes));
  EXPECT_EQ(0u, hashes.size());
  hashes.clear();

  TestFixture::CloseHistory(history2);

  History *history3 = TestFixture::OpenWritableHistory(hp);

  ASSERT_TRUE(history3->BeginTransaction());

  EXPECT_TRUE(history3->Remove("unobtainium"));
  EXPECT_EQ(3u, history3->GetNumberOfTags());
  ASSERT_TRUE(history3->ListRecycleBin(&hashes));
  EXPECT_EQ(0u, hashes.size());
  hashes.clear();

  EXPECT_TRUE(history3->Remove("bar"));
  EXPECT_EQ(2u, history3->GetNumberOfTags());
  ASSERT_TRUE(history3->ListRecycleBin(&hashes));
  EXPECT_EQ(1u, hashes.size());
  EXPECT_EQ(hashes[0], dummy_bar.root_hash);
  hashes.clear();

  EXPECT_TRUE(history3->Remove("not_here"));
  EXPECT_EQ(2u, history3->GetNumberOfTags());
  ASSERT_TRUE(history3->ListRecycleBin(&hashes));
  EXPECT_EQ(1u, hashes.size());
  hashes.clear();

  EXPECT_TRUE(history3->Remove("foo"));
  EXPECT_EQ(1u, history3->GetNumberOfTags());
  ASSERT_TRUE(history3->ListRecycleBin(&hashes));
  EXPECT_EQ(2u, hashes.size());
  if (hashes[0] == dummy_bar.root_hash) {
    EXPECT_EQ(hashes[0], dummy_bar.root_hash);
    EXPECT_EQ(hashes[1], dummy_foo.root_hash);
  } else {
    EXPECT_EQ(hashes[0], dummy_foo.root_hash);
    EXPECT_EQ(hashes[1], dummy_bar.root_hash);
  }
  hashes.clear();

  ASSERT_TRUE(history3->CommitTransaction());

  TestFixture::CloseHistory(history3);
}


TYPED_TEST(T_History, EmptyRecycleBin) {
  const std::string hp = TestFixture::GetHistoryFilename();
  History *history1 = TestFixture::CreateHistory(hp);
  ASSERT_NE(static_cast<History*>(NULL), history1);
  EXPECT_EQ(TestFixture::fqrn, history1->fqrn());

  ASSERT_TRUE(history1->BeginTransaction());
  History::Tag dummy_foo;
  dummy_foo.name      = "foo";
  dummy_foo.root_hash =
    shash::MkFromHexPtr(
      shash::HexPtr("5207a527a4fee2d655c67415aa1979f1d2753f96"),
      shash::kSuffixCatalog);
  dummy_foo.revision  = 1;
  dummy_foo.channel   = History::kChannelTest;
  ASSERT_TRUE(history1->Insert(dummy_foo));

  History::Tag dummy_bar;
  dummy_bar.name      = "bar";
  dummy_bar.root_hash =
    shash::MkFromHexPtr(
      shash::HexPtr("19552496e1e5c63aefaf5d4e05a8c248a1d82663"),
      shash::kSuffixCatalog);
  dummy_bar.revision  = 2;
  dummy_bar.channel   = History::kChannelTest;
  ASSERT_TRUE(history1->Insert(dummy_bar));

  History::Tag dummy_baz;
  dummy_baz.name      = "baz";
  dummy_baz.root_hash =
    shash::MkFromHexPtr(
      shash::HexPtr("400b66c2002e89629dd098918677e818e3688011"),
      shash::kSuffixCatalog);
  dummy_baz.revision  = 3;
  dummy_baz.channel   = History::kChannelTest;
  ASSERT_TRUE(history1->Insert(dummy_baz));
  EXPECT_TRUE(history1->CommitTransaction());

  EXPECT_EQ(3u, history1->GetNumberOfTags());

  std::vector<shash::Any> hashes;
  ASSERT_TRUE(history1->ListRecycleBin(&hashes));
  EXPECT_EQ(0u, hashes.size());
  hashes.clear();

  TestFixture::CloseHistory(history1);

  History *history2 = TestFixture::OpenWritableHistory(hp);
  ASSERT_NE(static_cast<History*>(NULL), history2);
  EXPECT_EQ(3u, history2->GetNumberOfTags());

  ASSERT_TRUE(history2->ListRecycleBin(&hashes));
  EXPECT_EQ(0u, hashes.size());
  hashes.clear();

  ASSERT_TRUE(history2->BeginTransaction());

  EXPECT_TRUE(history2->Remove("foo"));
  EXPECT_TRUE(history2->Remove("bar"));
  EXPECT_TRUE(history2->Remove("unobtainium"));

  EXPECT_EQ(1u, history2->GetNumberOfTags());
  ASSERT_TRUE(history2->ListRecycleBin(&hashes));
  EXPECT_EQ(2u, hashes.size());
  hashes.clear();

  EXPECT_TRUE(history2->EmptyRecycleBin());
  EXPECT_EQ(1u, history2->GetNumberOfTags());
  ASSERT_TRUE(history2->ListRecycleBin(&hashes));
  EXPECT_EQ(0u, hashes.size());
  hashes.clear();

  ASSERT_TRUE(history2->CommitTransaction());

  TestFixture::CloseHistory(history2);

  History *history3 = TestFixture::OpenWritableHistory(hp);
  ASSERT_NE(static_cast<History*>(NULL), history3);

  ASSERT_TRUE(history3->ListRecycleBin(&hashes));
  EXPECT_EQ(0u, hashes.size());
  hashes.clear();

  EXPECT_TRUE(history3->EmptyRecycleBin());
  EXPECT_EQ(1u, history3->GetNumberOfTags());
  ASSERT_TRUE(history3->ListRecycleBin(&hashes));
  EXPECT_EQ(0u, hashes.size());
  hashes.clear();

  TestFixture::CloseHistory(history3);
}


TYPED_TEST(T_History, RollbackAndRecycleBin) {
  const std::string hp = TestFixture::GetHistoryFilename();
  History *history1 = TestFixture::CreateHistory(hp);
  ASSERT_NE(static_cast<History*>(NULL), history1);
  EXPECT_EQ(TestFixture::fqrn, history1->fqrn());

  ASSERT_TRUE(history1->BeginTransaction());
  History::Tag dummy_foo;
  dummy_foo.name      = "foo";
  dummy_foo.root_hash =
    shash::MkFromHexPtr(
      shash::HexPtr("5207a527a4fee2d655c67415aa1979f1d2753f96"),
      shash::kSuffixCatalog);
  dummy_foo.revision  = 1;
  dummy_foo.channel   = History::kChannelTest;
  ASSERT_TRUE(history1->Insert(dummy_foo));

  History::Tag dummy_bar;
  dummy_bar.name      = "bar";
  dummy_bar.root_hash =
    shash::MkFromHexPtr(
      shash::HexPtr("19552496e1e5c63aefaf5d4e05a8c248a1d82663"),
      shash::kSuffixCatalog);
  dummy_bar.revision  = 2;
  dummy_bar.channel   = History::kChannelTest;
  ASSERT_TRUE(history1->Insert(dummy_bar));

  History::Tag dummy_baz;
  dummy_baz.name      = "baz";
  dummy_baz.root_hash =
    shash::MkFromHexPtr(
      shash::HexPtr("400b66c2002e89629dd098918677e818e3688011"),
      shash::kSuffixCatalog);
  dummy_baz.revision  = 3;
  dummy_baz.channel   = History::kChannelTest;
  ASSERT_TRUE(history1->Insert(dummy_baz));
  EXPECT_TRUE(history1->CommitTransaction());

  EXPECT_EQ(3u, history1->GetNumberOfTags());

  std::vector<shash::Any> hashes;
  ASSERT_TRUE(history1->ListRecycleBin(&hashes));
  EXPECT_EQ(0u, hashes.size());
  hashes.clear();

  TestFixture::CloseHistory(history1);

  History *history2 = TestFixture::OpenWritableHistory(hp);
  EXPECT_EQ(3u, history2->GetNumberOfTags());

  ASSERT_TRUE(history2->ListRecycleBin(&hashes));
  EXPECT_EQ(0u, hashes.size());
  hashes.clear();

  ASSERT_TRUE(history2->BeginTransaction());

  History::Tag rollback_target;
  ASSERT_TRUE(history2->GetByName("foo", &rollback_target));

  shash::Any new_root_hash(shash::kSha1);
  new_root_hash.Randomize();
  rollback_target.revision  = 4;
  rollback_target.root_hash = new_root_hash;
  EXPECT_TRUE(history2->Rollback(rollback_target));

  ASSERT_TRUE(history2->ListRecycleBin(&hashes));
  EXPECT_EQ(3u, hashes.size());

  std::vector<shash::Any>::const_iterator i    = hashes.begin();
  std::vector<shash::Any>::const_iterator iend = hashes.end();
  for (; i != iend; ++i) {
    shash::Any found_hash;
    if (*i == dummy_foo.root_hash) found_hash = dummy_foo.root_hash;
    if (*i == dummy_bar.root_hash) found_hash = dummy_bar.root_hash;
    if (*i == dummy_baz.root_hash) found_hash = dummy_baz.root_hash;
    EXPECT_FALSE(found_hash.IsNull());
    EXPECT_EQ(shash::kSuffixCatalog, found_hash.suffix);
  }
  hashes.clear();

  ASSERT_TRUE(history2->CommitTransaction());

  TestFixture::CloseHistory(history2);
}


TYPED_TEST(T_History, RecycleBinWithHeterogeneousHashes) {
  const std::string hp = TestFixture::GetHistoryFilename();
  History *history1 = TestFixture::CreateHistory(hp);
  ASSERT_NE(static_cast<History*>(NULL), history1);
  EXPECT_EQ(TestFixture::fqrn, history1->fqrn());

  ASSERT_TRUE(history1->BeginTransaction());
  History::Tag dummy_foo;
  shash::Any root_hash_sha(shash::kSha1); root_hash_sha.Randomize(1337);
  dummy_foo.name      = "foo";
  dummy_foo.root_hash = root_hash_sha;
  dummy_foo.revision  = 1;
  dummy_foo.channel   = History::kChannelTest;
  ASSERT_TRUE(history1->Insert(dummy_foo));

  History::Tag dummy_bar;
  shash::Any root_hash_rmd(shash::kRmd160); root_hash_rmd.Randomize(42);
  dummy_bar.name      = "bar";
  dummy_bar.root_hash = root_hash_rmd;
  dummy_bar.revision  = 2;
  dummy_bar.channel   = History::kChannelTest;
  ASSERT_TRUE(history1->Insert(dummy_bar));

  History::Tag dummy_baz;
  shash::Any root_hash_suf(shash::kSha1);
  root_hash_suf.Randomize(9);
  root_hash_suf.suffix = shash::kSuffixCatalog;
  dummy_baz.name = "baz";
  dummy_baz.root_hash = root_hash_suf;
  dummy_baz.revision = 3;
  dummy_baz.channel = History::kChannelTest;
  ASSERT_TRUE(history1->Insert(dummy_baz));
  EXPECT_TRUE(history1->CommitTransaction());

  EXPECT_EQ(3u, history1->GetNumberOfTags());

  std::vector<shash::Any> hashes;
  ASSERT_TRUE(history1->ListRecycleBin(&hashes));
  EXPECT_EQ(0u, hashes.size());
  hashes.clear();

  TestFixture::CloseHistory(history1);

  History *history2 = TestFixture::OpenWritableHistory(hp);
  EXPECT_EQ(3u, history2->GetNumberOfTags());

  ASSERT_TRUE(history2->ListRecycleBin(&hashes));
  EXPECT_EQ(0u, hashes.size());
  hashes.clear();

  ASSERT_TRUE(history2->BeginTransaction());

  EXPECT_TRUE(history2->Remove("foo"));
  EXPECT_TRUE(history2->Remove("unobtainium"));

  EXPECT_EQ(2u, history2->GetNumberOfTags());
  ASSERT_TRUE(history2->ListRecycleBin(&hashes));
  EXPECT_EQ(1u, hashes.size());
  EXPECT_EQ(dummy_foo.root_hash, hashes[0]);
  hashes.clear();

  ASSERT_TRUE(history2->EmptyRecycleBin());

  EXPECT_TRUE(history2->Remove("bar"));
  EXPECT_TRUE(history2->Remove("unobtainium"));

  EXPECT_EQ(1u, history2->GetNumberOfTags());
  ASSERT_TRUE(history2->ListRecycleBin(&hashes));
  EXPECT_EQ(1u, hashes.size());
  EXPECT_EQ(dummy_bar.root_hash, hashes[0]);
  hashes.clear();

  ASSERT_TRUE(history2->EmptyRecycleBin());

  EXPECT_TRUE(history2->Remove("baz"));
  EXPECT_TRUE(history2->Remove("unobtainium"));

  EXPECT_EQ(0u, history2->GetNumberOfTags());
  ASSERT_TRUE(history2->ListRecycleBin(&hashes));
  EXPECT_EQ(1u, hashes.size());
  EXPECT_EQ(dummy_baz.root_hash, hashes[0]);
  hashes.clear();

  ASSERT_TRUE(history2->CommitTransaction());

  TestFixture::CloseHistory(history2);
}


TYPED_TEST(T_History, ReadLegacyVersion1Revision0) {
  if (TestFixture::IsMocked()) {
    // this is only valid for the production code...
    // the mocked history does not deal with legacy formats
    return;
  }

  History *history = TestFixture::OpenHistory(TestFixture::history_v1_r0_path);
  ASSERT_NE(static_cast<History*>(NULL), history);

  EXPECT_EQ("config-egi.egi.eu", history->fqrn());
  EXPECT_TRUE(history->Exists("trunk"));

  History::Tag trunk;
  ASSERT_TRUE(history->GetByName("trunk", &trunk));
  EXPECT_EQ("trunk",                                       trunk.name);
  EXPECT_EQ(h("d13c98b4b48cedacda328eea4a30826333312c17"), trunk.root_hash);
  EXPECT_EQ(0u,                                            trunk.size);
  EXPECT_EQ(1u,                                            trunk.revision);
  EXPECT_EQ(1403013589,                                    trunk.timestamp);
  EXPECT_EQ(History::kChannelTrunk,                        trunk.channel);

  std::vector<History::Tag> tags;
  ASSERT_TRUE(history->List(&tags));
  EXPECT_EQ(1u, tags.size());

  std::vector<shash::Any> recycled_hashes;
  EXPECT_FALSE(history->ListRecycleBin(&recycled_hashes));

  TestFixture::CloseHistory(history);
}


TYPED_TEST(T_History, ReadLegacyVersion1Revision1) {
  if (TestFixture::IsMocked()) {
    // this is only valid for the production code...
    // the mocked history does not deal with legacy formats
    return;
  }

  History *history = TestFixture::OpenHistory(TestFixture::history_v1_r1_path);
  ASSERT_NE(static_cast<History*>(NULL), history);

  EXPECT_EQ("fcc.cern.ch", history->fqrn());
  EXPECT_TRUE(history->Exists("trunk"));
  EXPECT_TRUE(history->Exists("trunk-previous"));

  History::Tag trunk_p;
  ASSERT_TRUE(history->GetByName("trunk-previous", &trunk_p));
  EXPECT_EQ("trunk-previous",                              trunk_p.name);
  EXPECT_EQ(h("87664f7d3c4a75a8c9f62e3a47b962c001d6b52a"), trunk_p.root_hash);
  EXPECT_EQ(14336u,                                        trunk_p.size);
  EXPECT_EQ(2u,                                            trunk_p.revision);
  EXPECT_EQ(1416826665,                                    trunk_p.timestamp);
  EXPECT_EQ(History::kChannelTrunk,                        trunk_p.channel);

  std::vector<History::Tag> tags;
  ASSERT_TRUE(history->List(&tags));
  EXPECT_EQ(2u, tags.size());
  EXPECT_TRUE((tags[0].name == "trunk" && tags[1].name == "trunk-previous") ||
              (tags[1].name == "trunk" && tags[0].name == "trunk-previous"));

  std::vector<shash::Any> recycled_hashes;
  EXPECT_FALSE(history->ListRecycleBin(&recycled_hashes));

  TestFixture::CloseHistory(history);
}


TYPED_TEST(T_History, UpgradeAndWriteLegacyVersion1Revision0Slow) {
  if (TestFixture::IsMocked()) {
    // this is only valid for the production code...
    // the mocked history does not deal with legacy formats
    return;
  }

  History *history = TestFixture::OpenWritableHistory(
                                               TestFixture::history_v1_r0_path);
  ASSERT_NE(static_cast<History*>(NULL), history);

  const History::Tag dummy = TestFixture::GetDummyTag();
  ASSERT_TRUE(history->Insert(dummy));
  EXPECT_EQ(2u, history->GetNumberOfTags());

  History::Tag tag;
  ASSERT_TRUE(history->GetByName(dummy.name, &tag));
  TestFixture::CompareTags(dummy, tag);

  std::vector<shash::Any> recycled_hashes;
  ASSERT_TRUE(history->ListRecycleBin(&recycled_hashes));
  EXPECT_EQ(0u, recycled_hashes.size());

  ASSERT_TRUE(history->Remove(dummy.name));
  EXPECT_EQ(1u, history->GetNumberOfTags());

  ASSERT_TRUE(history->ListRecycleBin(&recycled_hashes));
  EXPECT_EQ(1u, recycled_hashes.size());
  EXPECT_EQ(dummy.root_hash, recycled_hashes[0]);

  ASSERT_TRUE(history->EmptyRecycleBin());

  ASSERT_TRUE(history->ListRecycleBin(&recycled_hashes));
  EXPECT_EQ(0u, recycled_hashes.size());

  TestFixture::CloseHistory(history);
}


TYPED_TEST(T_History, UpgradeAndWriteLegacyVersion1Revision1Slow) {
  if (TestFixture::IsMocked()) {
    // this is only valid for the production code...
    // the mocked history does not deal with legacy formats
    return;
  }

  History *history = TestFixture::OpenWritableHistory(
                                               TestFixture::history_v1_r1_path);
  ASSERT_NE(static_cast<History*>(NULL), history);

  const History::Tag dummy = TestFixture::GetDummyTag();
  ASSERT_TRUE(history->Insert(dummy));
  EXPECT_EQ(3u, history->GetNumberOfTags());

  History::Tag tag;
  ASSERT_TRUE(history->GetByName(dummy.name, &tag));
  TestFixture::CompareTags(dummy, tag);

  std::vector<shash::Any> recycled_hashes;
  ASSERT_TRUE(history->ListRecycleBin(&recycled_hashes));
  EXPECT_EQ(0u, recycled_hashes.size());

  ASSERT_TRUE(history->Remove(dummy.name));
  EXPECT_EQ(2u, history->GetNumberOfTags());

  ASSERT_TRUE(history->ListRecycleBin(&recycled_hashes));
  EXPECT_EQ(1u, recycled_hashes.size());
  EXPECT_EQ(dummy.root_hash, recycled_hashes[0]);

  ASSERT_TRUE(history->EmptyRecycleBin());

  ASSERT_TRUE(history->ListRecycleBin(&recycled_hashes));
  EXPECT_EQ(0u, recycled_hashes.size());

  TestFixture::CloseHistory(history);
}
