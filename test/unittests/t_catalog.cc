/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>
#include <sys/stat.h>
#include <unistd.h>

#include "catalog.h"
#include "catalog_rw.h"
#include "compression/compression.h"
#include "crypto/hash.h"
#include "shortstring.h"
#include "testutil.h"
#include "util/posix.h"

using namespace std;  // NOLINT

namespace catalog {

class T_Catalog : public ::testing::Test {
 protected:
  virtual void SetUp() {
    catalog = NULL;
    nested = NULL;
    sandbox = CreateTempDir("./cvmfs_ut_catalog");
    ASSERT_FALSE(sandbox.empty());
    FileChunk file_chunk;

    // creating firstly the father
    string root_path = "";
    catalog_db_root = CreateCatalogDB(root_path);
    catalog::WritableCatalog
        *writable_root_catalog = catalog::WritableCatalog::AttachFreely(
            root_path, catalog_db_root, shash::Any(shash::kSha1), NULL, false);
    AddEntry(writable_root_catalog, "foo", "", S_IFREG,
             "988881adc9fc3655077dc2d4d757d480b5ea0e11", "", true);
    writable_root_catalog->AddFileChunk("/foo", file_chunk);
    AddEntry(writable_root_catalog, "hidden", "", S_IFDIR, "", "", false, true);
    AddEntry(writable_root_catalog, "dir", "", S_IFDIR, "");
    AddEntry(writable_root_catalog, "dir", "/dir", S_IFDIR, "");
    AddEntry(writable_root_catalog, "folder", "/dir", S_IFDIR, "");
    AddEntry(writable_root_catalog, "bar", "/dir/dir", S_IFREG,
             "448fa8e3d2b1a80d4f38727cd9a85eb2c0faf433");
    AddEntry(writable_root_catalog, "bar2", "/dir/dir", S_IFREG,
             "1e12aecf3c6b0e9208cf5a22e3d5dec7edbd577e");
    AddEntry(writable_root_catalog, "link", "/dir/dir", S_IFLNK, "", "/foo");

    // secondly the child
    shash::Any child_hash = shash::Any(shash::kSha1);
    nested_path = "/dir/folder";
    catalog_db_nested = CreateCatalogDB(nested_path);
    catalog::WritableCatalog
        *writable_nested_catalog = catalog::WritableCatalog::AttachFreely(
            nested_path,
            catalog_db_nested,
            child_hash,
            writable_root_catalog,
            true);
    AddEntry(writable_nested_catalog, "file1", nested_path, S_IFREG,
             "38be7d1b981f2fb6a4a0a052453f887373dc1fe8");
    AddEntry(writable_nested_catalog, "file2", nested_path, S_IFREG,
             "639daad06642a8eb86821ff7649e86f5f59c6139");

    // commit the son (nested)
    writable_nested_catalog->Commit();
    delete writable_nested_catalog;
    ASSERT_TRUE(shash::HashFile(catalog_db_nested, &child_hash));
    uint64_t size = GetFileSize(catalog_db_nested);

    // commit the father (root)
    writable_root_catalog->InsertNestedCatalog(
        nested_path, NULL, child_hash, size);
    writable_root_catalog->Commit();
    delete writable_root_catalog;
  }

  string CreateCatalogDB(const string &root_path) {
    string db_file = CreateTempPath(sandbox + "/catalog", 0666);
    EXPECT_FALSE(db_file.empty());

    const bool volatile_content = false;
    {
      UniquePtr<catalog::CatalogDatabase> new_clg_db(
          catalog::CatalogDatabase::Create(db_file));
      EXPECT_TRUE(new_clg_db.IsValid());
      bool retval = new_clg_db->InsertInitialValues(root_path, volatile_content,
                                                    "");
      EXPECT_TRUE(retval);
    }
    return db_file;
  }

  void AddEntry(catalog::WritableCatalog *writable_catalog,
                string name,
                string parent_path,
                unsigned mode,  // S_IFREG, S_IFDIR
                string checksum,
                string symlink = "",
                bool is_chunked_file = false,
                bool is_hidden = false) {
    catalog::DirectoryEntryTestFactory::Metadata metadata;
    metadata.name = name;
    metadata.mode = mode | S_IRWXU;  // file permissions/mode
    metadata.uid = getuid();
    metadata.gid = getgid();
    metadata.size = 4 * 1024;
    metadata.mtime = IsoTimestamp2UtcTime("2015-06-19T09:10:18Z");
    metadata.symlink = symlink;
    metadata.linkcount = 1;
    metadata.has_xattrs = false;
    metadata.checksum = shash::MkFromHexPtr(shash::HexPtr(checksum));
    metadata.is_hidden = is_hidden;
    string complete_name = parent_path + "/" + name;
    DirectoryEntry de(DirectoryEntryTestFactory::Make(metadata));
    XattrList xattrlist;
    de.set_is_chunked_file(is_chunked_file);
    writable_catalog->AddEntry(de, xattrlist, complete_name, parent_path);
    writable_catalog->UpdateLastModified();
  }

  virtual void TearDown() {
    delete catalog;
    delete nested;
    RemoveTree(sandbox);
  }

  Catalog *catalog;
  Catalog *nested;
  string sandbox;
  string nested_path;
  string catalog_db_root;
  string catalog_db_nested;
};


TEST_F(T_Catalog, NormalizePath) {
  catalog::Catalog c1(PathString(""), shash::Any(), NULL, false);
  EXPECT_TRUE(c1.is_regular_mountpoint_);
  EXPECT_EQ(shash::Md5(shash::AsciiPtr("")), c1.NormalizePath(PathString("")));
  EXPECT_EQ(PathString(""), c1.NormalizePath2(PathString("")));
  EXPECT_EQ(shash::Md5(shash::AsciiPtr("/foo/bar")),
            c1.NormalizePath(PathString("/foo/bar")));
  EXPECT_EQ(PathString("/foo/bar"), c1.NormalizePath2(PathString("/foo/bar")));

  catalog::Catalog c2(PathString("/.cvmfs"), shash::Any(), NULL, false);
  EXPECT_FALSE(c2.is_regular_mountpoint_);
  EXPECT_EQ(PathString(""), c2.root_prefix_);
  EXPECT_EQ(shash::Md5(shash::AsciiPtr("")),
            c2.NormalizePath(PathString("/.cvmfs")));
  EXPECT_EQ(PathString(""), c2.NormalizePath2(PathString("/.cvmfs")));
  EXPECT_EQ(shash::Md5(shash::AsciiPtr("/foo/bar")),
            c2.NormalizePath(PathString("/.cvmfs/foo/bar")));
  EXPECT_EQ(PathString("/foo/bar"),
            c2.NormalizePath2(PathString("/.cvmfs/foo/bar")));

  catalog::Catalog c3(PathString("/.cvmfs/nested"), shash::Any(), NULL, false);
  EXPECT_FALSE(c3.is_regular_mountpoint_);
  c3.root_prefix_ = PathString("/nested");
  EXPECT_EQ(shash::Md5(shash::AsciiPtr("/nested")),
            c3.NormalizePath(PathString("/.cvmfs/nested")));
  EXPECT_EQ(PathString("/nested"),
            c3.NormalizePath2(PathString("/.cvmfs/nested")));
  EXPECT_EQ(shash::Md5(shash::AsciiPtr("/nested/foo/bar")),
            c3.NormalizePath(PathString("/.cvmfs/nested/foo/bar")));
  EXPECT_EQ(PathString("/nested/foo/bar"),
            c3.NormalizePath2(PathString("/.cvmfs/nested/foo/bar")));
}


TEST_F(T_Catalog, PlantPath) {
  catalog::Catalog c1(PathString(""), shash::Any(), NULL, false);
  EXPECT_TRUE(c1.is_regular_mountpoint_);
  EXPECT_EQ(PathString(""), c1.PlantPath(PathString("")));
  EXPECT_EQ(PathString("/foo/bar"), c1.PlantPath(PathString("/foo/bar")));

  catalog::Catalog c2(PathString("/.cvmfs"), shash::Any(), NULL, false);
  EXPECT_EQ(PathString(""), c2.root_prefix_);
  EXPECT_FALSE(c2.is_regular_mountpoint_);
  EXPECT_EQ(PathString("/.cvmfs"), c2.PlantPath(PathString("")));
  EXPECT_EQ(PathString("/.cvmfs/nested"), c2.PlantPath(PathString("/nested")));

  catalog::Catalog c3(PathString("/.cvmfs/nested"), shash::Any(), NULL, false);
  EXPECT_FALSE(c3.is_regular_mountpoint_);
  c3.root_prefix_ = PathString("/nested");
  EXPECT_EQ(PathString("/.cvmfs/nested"), c3.PlantPath(PathString("/nested")));
  EXPECT_EQ(PathString("/.cvmfs/nested/foo/bar"),
            c3.PlantPath(PathString("/nested/foo/bar")));
}


TEST_F(T_Catalog, Attach) {
  catalog = catalog::Catalog::AttachFreely(
      "", catalog_db_root, shash::Any(), NULL, false);
  EXPECT_NE(static_cast<Catalog *>(NULL), catalog);
  EXPECT_TRUE(catalog->IsInitialized());
  EXPECT_TRUE(catalog->IsRoot());
  EXPECT_FALSE(catalog->IsWritable());
  EXPECT_FALSE(catalog->HasParent());
  EXPECT_EQ(NULL, catalog->parent());
  EXPECT_EQ(0u, catalog->revision());
  EXPECT_FALSE(catalog->volatile_flag());
  EXPECT_EQ(shash::Any(), catalog->hash());
  EXPECT_EQ(shash::Any(), catalog->GetPreviousRevision());
  EXPECT_EQ(catalog_db_root, catalog->database_path());
  EXPECT_NE(0u, catalog->GetTTL());
  EXPECT_EQ(8u, catalog->GetNumEntries());
  EXPECT_EQ(8u, catalog->max_row_id());

  EXPECT_FALSE(catalog->OwnsDatabaseFile());
  catalog->TakeDatabaseFileOwnership();
  EXPECT_TRUE(catalog->OwnsDatabaseFile());
  catalog->DropDatabaseFileOwnership();
  EXPECT_FALSE(catalog->OwnsDatabaseFile());

  Catalog::HashVector hash_vector(catalog->GetReferencedObjects());
  EXPECT_FALSE(hash_vector.empty());
  EXPECT_EQ(shash::kSha1, hash_vector[0].algorithm);

  shash::Any hash;
  uint64_t size;
  EXPECT_TRUE(catalog->FindNested(PathString(nested_path), &hash, &size));
  nested = catalog::Catalog::AttachFreely(
      nested_path, catalog_db_nested, shash::Any(), catalog, true);
  EXPECT_TRUE(nested->IsInitialized());
  EXPECT_FALSE(nested->IsRoot());
  EXPECT_FALSE(nested->IsWritable());
  EXPECT_TRUE(nested->HasParent());
  EXPECT_EQ(catalog, nested->parent());
  EXPECT_EQ(2u, nested->GetNumEntries());
  EXPECT_EQ(NULL,
            catalog::Catalog::AttachFreely("", "/random/path", shash::Any()));
}

TEST_F(T_Catalog, OpenDatabase) {
  catalog = new Catalog(PathString(""), shash::Any(), NULL);
  EXPECT_FALSE(catalog->OpenDatabase("/fake/path/to/db"));
  EXPECT_FALSE(catalog->OwnsDatabaseFile());
  catalog->TakeDatabaseFileOwnership();

  // initializing the database
  EXPECT_TRUE(catalog->OpenDatabase(catalog_db_root));
  EXPECT_TRUE(catalog->OwnsDatabaseFile());
  EXPECT_EQ(catalog_db_root, catalog->database_path());
  EXPECT_NE(0u, catalog->GetLastModified());
}

TEST_F(T_Catalog, Lookup) {
  catalog = new Catalog(PathString(""), shash::Any(), NULL);
  PathString fake_path("/fakepath/fakefile");
  PathString path("/dir/dir");
  PathString hidden_path("/hidden");
  PathString link_path("/dir/dir/link");
  DirectoryEntry dirent;
  shash::Md5 md5_hash;
  XattrList xattr_list;
  EXPECT_DEATH(catalog->LookupPath(fake_path, NULL), ".*");
  EXPECT_DEATH(catalog->LookupXattrsPath(fake_path, &xattr_list), ".*");

  delete catalog;
  catalog = catalog::Catalog::AttachFreely(
      "", catalog_db_root, shash::Any(), NULL, false);
  EXPECT_FALSE(catalog->LookupPath(fake_path, &dirent));
  EXPECT_TRUE(catalog->LookupPath(path, &dirent));
  EXPECT_TRUE(dirent.IsDirectory());
  EXPECT_FALSE(dirent.IsHidden());
  EXPECT_FALSE(dirent.IsBindMountpoint());
  EXPECT_TRUE(dirent.symlink().IsEmpty());
  EXPECT_FALSE(dirent.IsChunkedFile());
  EXPECT_FALSE(dirent.IsLink());
  EXPECT_FALSE(dirent.IsNegative());
  EXPECT_FALSE(dirent.IsNestedCatalogMountpoint());
  EXPECT_FALSE(dirent.IsRegular());
  EXPECT_FALSE(dirent.IsNestedCatalogRoot());
  EXPECT_EQ(NameString("dir"), dirent.name());
  EXPECT_EQ(4096u, dirent.size());
  EXPECT_EQ(getuid(), dirent.uid());
  EXPECT_EQ(getgid(), dirent.gid());
  EXPECT_EQ(1u, dirent.linkcount());

  EXPECT_TRUE(catalog->LookupXattrsPath(path, &xattr_list));
  EXPECT_TRUE(xattr_list.IsEmpty());  // no xattrs in the directory

  LinkString file;
  EXPECT_TRUE(catalog->LookupRawSymlink(link_path, &file));
  EXPECT_EQ("/foo", file.ToString());

  EXPECT_TRUE(catalog->LookupPath(hidden_path, &dirent));
  EXPECT_TRUE(dirent.IsHidden());
}

TEST_F(T_Catalog, Listing) {
  StatEntryList stat_entry_list;
  DirectoryEntryList dir_entry_list;
  PathString fake_path("/fakepath/fakefile");
  PathString path("/dir/dir");
  PathString root_path("");
  catalog = catalog::Catalog::AttachFreely(
      "", catalog_db_root, shash::Any(), NULL, false);

  EXPECT_TRUE(catalog->ListingPathStat(fake_path, &stat_entry_list));
  EXPECT_TRUE(stat_entry_list.IsEmpty());

  EXPECT_TRUE(catalog->ListingPathStat(path, &stat_entry_list));
  ASSERT_EQ(3u, stat_entry_list.size());
  EXPECT_EQ(NameString("bar"), stat_entry_list.AtPtr(0)->name);
  EXPECT_EQ(NameString("bar2"), stat_entry_list.AtPtr(1)->name);
  EXPECT_EQ(NameString("link"), stat_entry_list.AtPtr(2)->name);

  EXPECT_TRUE(catalog->ListingPath(fake_path, &dir_entry_list));
  EXPECT_TRUE(dir_entry_list.empty());

  EXPECT_TRUE(catalog->ListingPath(path, &dir_entry_list));
  ASSERT_EQ(3u, dir_entry_list.size());
  EXPECT_EQ(NameString("bar"), dir_entry_list.at(0).name());
  EXPECT_EQ(NameString("bar2"), dir_entry_list.at(1).name());
  EXPECT_EQ(NameString("link"), dir_entry_list.at(2).name());

  Catalog::NestedCatalogList nc_list = catalog->ListNestedCatalogs();
  EXPECT_EQ(1u, nc_list.size());
  EXPECT_EQ(PathString("/dir/folder"), nc_list.at(0).mountpoint);

  FileChunkList file_chunk_list;
  EXPECT_TRUE(catalog->ListPathChunks(
      PathString("/foo"), shash::kSha1, &file_chunk_list));
  DirectoryEntry chunk_dir_entry;
  ASSERT_EQ(1u, file_chunk_list.size());
  ASSERT_TRUE(catalog->LookupPath(PathString("/foo"), &chunk_dir_entry));
  EXPECT_TRUE(chunk_dir_entry.IsChunkedFile());
  EXPECT_EQ(0u, file_chunk_list.AtPtr(0)->size());
  EXPECT_EQ(0u, file_chunk_list.AtPtr(0)->offset());

  EXPECT_TRUE(catalog->ListingPath(root_path, &dir_entry_list));
  EXPECT_FALSE(dir_entry_list.empty());
  bool hidden_found = false;
  for (unsigned i = 0; i < dir_entry_list.size(); ++i) {
    if (dir_entry_list[i].name() == NameString("hidden")) {
      hidden_found = true;
      break;
    }
  }
  EXPECT_TRUE(hidden_found);
  StatEntryList root_stat_entry_list;
  EXPECT_TRUE(catalog->ListingPathStat(root_path, &root_stat_entry_list));
  EXPECT_FALSE(root_stat_entry_list.IsEmpty());
  for (unsigned i = 0; i < root_stat_entry_list.size(); ++i)
    EXPECT_NE(NameString("hidden"), root_stat_entry_list.At(i).name);
}

TEST_F(T_Catalog, Chunks) {
  catalog = catalog::Catalog::AttachFreely(
      "", catalog_db_root, shash::Any(), NULL, false);
  shash::Any hash;
  zlib::Algorithms compression_alg;
  EXPECT_TRUE(catalog->AllChunksBegin());
  unsigned counter = 0;
  while (catalog->AllChunksNext(&hash, &compression_alg)) {
    ++counter;
    EXPECT_EQ(zlib::kZlibDefault, compression_alg);
  }
  EXPECT_TRUE(catalog->AllChunksEnd());
  EXPECT_EQ(4u, counter);  // number of files with content + empty hash
}

TEST_F(T_Catalog, Statistics) {
  catalog = catalog::Catalog::AttachFreely(
      "", catalog_db_root, shash::Any(), NULL, false);
  EXPECT_NE("", catalog->PrintMemStatistics());
}

namespace {
// Compressed and slimmed catalog from the NA61 repository that lacks the
// nested catalog SHA-1 field.
std::string kCatalogNa61Schema09 =
    "eJztWg1UU9cdf/"
    "feJJCEtFbLMuBYHmudUPkO34oQMBJGCF+hgtXFQB4QIRCS4AGkVdRo2bFYN0pR"
    "N+c6p7bVUw+jjjK/pgc/cGi79jhbXW272qprPHXtWtfVufveCx/vPTJ7draebb7fOcn7v1/"
    "u7597"
    "78v939z/"
    "vWUlBpubImuanHaLm9QQEgIAIpskCdDqJghCRUxgFn5JfLYCvwBxN9A+YuMzpMS4UnlX"
    "jQgRIr4xgK/5Ro9hQL8h5u0brqUIESL+E2DGNforbarYiwgRIu4FBG0e0dB/"
    "5B+E3yHQVfQaGkQ7"
    "0FPIgSpQNopE0+At+C48BfvhVrgGLoelMAMXFHGPQS2RyZDzRPiB8tEd+"
    "ojm1OeGZ0hPAw9keKr8"
    "zkMBT/T+dGD4wVPfr35WKg11Mvzi/YmOp0MfG8z+Tfm5DYOfl8K8cwy/aHbZnkNdfR/"
    "06+T1xX9f"
    "ZpeGuhh+1nCoG17apBq6GLF0g/"
    "ztEeBRsrxipuLss+3vdT+ntGbtK84CHhSMeSDpJza9tXHmm0/m"
    "3Pq4T776GmBK3+zYtWDulYMvnzvxVeu+XeeaidEkhv8wZ2BV4wXP6zcODb622/"
    "u7r6RhMxn+7PLr"
    "365XpH/2xieuJN3VtV4JpWH4k+HK9gr15y8mG2rIrid0HdLQZoY/"
    "kVwt7wy8z9x5Y1j5cNadz4CH"
    "7Z3jl/"
    "WrtuYu+WLDZr3DmZ8TCTxBDH+kct0Xjh0zlh6bh7rbr2b+SRq6guFftdb2vFqS0VO8e8Oq"
    "juvK9dLgFobfVXel/tPG6r/"
    "k9V3sHYLLtwOPnOG39P9abzp8+IplWvMrl+EVJfDIGP7pyvdbbTGR"
    "T+174cj3Qh9VvQQ8Cobvisr+8DFbrNM8uj2ke/iTxcATyPBrFm//uEiTomy7WNJ7u/"
    "4jF9TH0At8"
    "hPoI9Gd0Hl9EiBDxX4B5ehS31OGkVtiaWlxm+"
    "uqyNTUm11g1VLo1xWpNT6mpqUrUJCam1Vjiq6i0"
    "NE1ySlWqJimVSktIpkLz0OyIBovLbbY3WW01NsqakJiemp6cnp6SGLQQhcxwVddRdktCbDw9/"
    "yM0"
    "SqAb6AK+iBAh4n8VChQC2JG9MBjFAUH40D+AZgNOWMhjEvySuzkWIULE/yPo+V8c/"
    "yJE3Jtg83/x"
    "/yz/tx/2wdWwDi6DelxQxD0NJo8kyL55puaJ0aQACQCAIMCU+bOxPJyA9+XJhLzLD+/"
    "0w0+dtxvL"
    "t/H5sfwfn4f6mKl5X1ZTmBcM8sMr/fAKP7zcDx/"
    "oh5f54ZEfHjLjH80n0JfoJo4B76A30Qg6ig6g"
    "veh51Ie60TrUjiNCDVqCSpEeFxQhQoQPcoRRp8NxjLUewZGLsWoP41jFWkM4OrFWH45HjGU8ii"
    "MQ"
    "ax3CMYe2oNGCowZrFeM4wVoGHBlYKxfHAtZKx6OftULweGeswuN4hLPWIB7TrLUNR2XaAsuCcR"
    "wO"
    "RBBlbsQBizY0LTiiSREAEqBCt4hAopGAv0CnwafQAaLBADGMiX8XqIAAdUIY6FTaGq1UeaBMHR"
    "cG"
    "mmi71WZtNVdb3JaGplqzrbHJSvluFLmlOq1JR+YbF+gqSEEpsshI+ggykmGiON/"
    "B8euwOKlGt+9O"
    "7tcxW4zjmaXMCdGkz0qM6pRKA9Rz5oBOrdtS1UA1Ui43ZR3z4eLdBvi+"
    "zKTNMehI3oe0e3cdadJV"
    "mKLJ3CJjmalUm280kY56M79kcWl+"
    "oba0kizQVbKqqKhMmUxdMgcQTGtdzQ02N2W2tLjZXuU7MCfw"
    "iMCVKEAdETHWCoezyUE53TbKNWFJOXWf4MnIeqrNV+sVloYWasoWTCrPqTzWRkVpJDJ1XoS/"
    "uk9I"
    "zQkTtmxNBAhQh4WBdSFMlX0t8V0Qp7Ljz89uTaZ7y5yAH7dJl6crjSbHqMQJauwpC5hJZdhf3f"
    "ht"
    "ncVVR+YYinKiSZetfdIHdk4xu9tmn3Rb02DBD3P8ttFiH+s8V5u9wdZYP2VXjrWG04/"
    "jLZvUoqio"
    "GChTa8P8dezYDz3BZ9Ar/kB6eNL5v/"
    "uIQgKdRz9C5UgN34Lb4FIYDt4DPwcUUBB78YdfF49vHtGo"
    "ZBgoCSIJmK3ib7Dy/"
    "4Dg+Jef+cxQ9KPNsg9GXt95RlI5re3SmSVFsUTnzoJrC38rqaMc1qpY3JzY"
    "+PiExV3PxDDuoc89fz+T717TckH/vPp6iV5Tr3/l+kurM1pSq/ZtMnbS3t8NaeN4j1/"
    "UtanI5x1h"
    "7+Eq/h4233vmxt0//smS86fn3Tyyl9oT/ssm+ZmjZ++g39fT7teeHZbY22jX+JWx/"
    "gdBbL8QAErA"
    "Ayr+hrHgj6XRotR6sx46pgCmUqxWctT8zXKhuni61jv/"
    "0ql+YDJgtYKj5m8aC9UGqdabtmful8CU"
    "h9Vyjpq/"
    "RS1U5yq03qSD2w6CsnysDuSo+RvTQnU63e5vrbwEiouxWsZR87fBheoQiGu+an4RyDdi"
    "NeL2Gu8ogUBdeHyG1puzfH85WFCB1ZCj5h+AEKoHg+"
    "iadzwJcnOxmuCoBQsN3kIJz9b3a70FLu9l"
    "YK3KWrs1aZI6TMU/1iBYfi0LlmtvF7jifgUabFVOi7MtEmAH9MKLdqBSCdZlhARiQc2F6YAe/"
    "zJo"
    "JNDPUDWaBT+CL+AbESL+JSzqOTBz0gQQruKfvBFMAHW6l7PfKCjbIt1SFfC3yrKVWXNrizrfL/"
    "0u"
    "0TlguLb99ESIzugZWMEZU/zDNULXj0RovQaq435QuBCrXRw1/yCSQF17uBKr/"
    "9h7A+TpsNrJUfOP"
    "PQnVQziGGlTxJMil1c0cNf94kVDd9zBWv70zBORqH+95sYUzp/IPDwnn1KM/dCe/"
    "c7t7aP30zGMR"
    "XSf/YOw4trezuBdPS4Z1J+M4s17iPwAwivLb";

// Compressed and slimmed catalog from the NA61 repository that _has_ the
// nested catalog SHA-1 field, to make sure we apply the fix only where
// necessary.
std::string kCatalogNa61Schema10 =
    "eJztmttqE0EYgGcyObU14IGy1CBMbmxDa7ObTasVBZO6lDS1tWnE9qIs2+"
    "zELM0mMbstbQUx4qXg"
    "bV/AFxCvfAMv9EpKfQRFvFAQEQRns5vDpq0WBLHtfMxk/vl35t/"
    "5Z3YOm2RhfkYzCS5UarpiYhF4"
    "AYTgBsYAbmwBAPpAm4s0eh25l0YI/oRlY5S/6gOtmn2/"
    "Lc9gMP4p8JAfPnCYCc9gMI4SIRoh2gE0"
    "MBiMY8EQDPn9kJ7koccLQyHvC/Ds/dML7x6mvn/"
    "a7nn0gZ7GPcmfmcLuWdg4yKOPgAYGg3Fs6ffS"
    "JaF7JYDW/o/"
    "QNkBf0A5NGAzGf8C1DIotV2tkXausGbKVGlqlLMSvqAmeKKKY58kKPxbnaaqSFTI2"
    "nhDzE3FF5MWJvFo4P40GIyXFMGW9omoFjaiCyI8LlxPiROJUGg2cM/JFoivCKG/t/"
    "wi9Aegz2qUJ"
    "g8E4qvSiAWjP7HQ/isE9y0fmDBqErmVh2n7/fwtoYDAYRxoeIm45VlXMIn8oEnH7/"
    "f8boIHBYJwI"
    "ehAHG8uE8/3fd0ADg8E4IQS81s8DwJn/"
    "PwANDAbjxOBDEHphiG78QZAHnufoNfzqqcIIfAleUcVf"
    "QQIBTgjDep9WVsmdoJ+LhWHFkjc0dUPOK6ZSqtyTtXJFJU6mdzIrJXMSTs/"
    "elBbxnlJ4bhY7CjzU"
    "0ERd93DZrSo1UjadXM+Bhu1iLsu2ShZGsCPFo/"
    "XTvgA3PAzrd01lpUTKxDCJ2rRhdGUDzs1yydSM"
    "hLsuWubNIs5Ji7kRbBQVwREn52YXctlkejaHq6tyd6Xb2fStZHYJZ6Ql20A0et3v5+"
    "aHIWg4btwv"
    "aSaRlTXT7uBuA7LQpQg+"
    "QAEuEoH1ZMOhaq1SJTVTI0Zb8rncaOvx0CrZdFq9rpTWyL4edJR3NZ7W"
    "jUZFr5+bihzU9nZVWWjL/"
    "scRGODCYfhkoNFkxxMnQa7GtoZSV8es3pIFOvI5aUrKjuCmKt5WNQd8"
    "j6ajjP0AtrJFxSji1Mxcig6ittVxQXcV001N78gWSgodzFa2rOjNzjM29ZJWXt23K5veuPqx5V"
    "mH"
    "R9HoJY+fS4YP6tjmMy84gvVv3KA1U38BKyOiUA==";
}  // anonymous namespace

TEST_F(T_Catalog, AttachSchema09) {
  std::string catalog_debase;
  bool retval = Debase64(kCatalogNa61Schema09, &catalog_debase);
  ASSERT_TRUE(retval);

  void *catalog_binary;
  uint64_t catalog_size;
  retval = zlib::DecompressMem2Mem(catalog_debase.data(),
                                   catalog_debase.length(), &catalog_binary,
                                   &catalog_size);
  ASSERT_TRUE(retval);

  std::string temp_path;
  FILE *f = CreateTempFile("cvmfs_ut_legacy", 0666, "w+", &temp_path);
  EXPECT_TRUE(f != NULL);
  if (f == NULL) {
    free(catalog);
    return;
  }
  retval = SafeWrite(fileno(f), catalog_binary, catalog_size);
  free(catalog_binary);
  fclose(f);
  EXPECT_TRUE(retval);

  WritableCatalog *catalog = WritableCatalog::AttachFreely("", temp_path,
                                                           shash::Any());
  unlink(temp_path.c_str());
  EXPECT_TRUE(catalog != NULL);

  int nchunk = 0;
  catalog->AllChunksBegin();
  shash::Any h;
  zlib::Algorithms a;
  while (catalog->AllChunksNext(&h, &a)) {
    nchunk++;
  }
  catalog->AllChunksEnd();
  EXPECT_EQ(5, nchunk);
  EXPECT_EQ(5U, catalog->GetReferencedObjects().size());

  uint64_t s;
  EXPECT_FLOAT_EQ(0.9, catalog->schema());
  EXPECT_EQ(0U, catalog->ListOwnNestedCatalogs().size());
  EXPECT_EQ(0U, catalog->ListNestedCatalogs().size());
  EXPECT_FALSE(catalog->FindNested(PathString("/path"), &h, &s));
  EXPECT_TRUE(catalog->IsRoot());
  EXPECT_EQ(1297959962U, catalog->GetLastModified());
  shash::Any previous_revision = shash::MkFromHexPtr(
      shash::HexPtr("5fd3e9d6dd96ffb23228fa0be88356b7347e815e"));
  EXPECT_EQ(catalog->GetPreviousRevision(), previous_revision);
}

TEST_F(T_Catalog, AttachSchema10) {
  std::string catalog_debase;
  bool retval = Debase64(kCatalogNa61Schema10, &catalog_debase);
  ASSERT_TRUE(retval);

  void *catalog_binary;
  uint64_t catalog_size;
  retval = zlib::DecompressMem2Mem(catalog_debase.data(),
                                   catalog_debase.length(), &catalog_binary,
                                   &catalog_size);
  ASSERT_TRUE(retval);

  std::string temp_path;
  FILE *f = CreateTempFile("cvmfs_ut_legacy", 0666, "w+", &temp_path);
  EXPECT_TRUE(f != NULL);
  if (f == NULL) {
    free(catalog);
    return;
  }
  retval = SafeWrite(fileno(f), catalog_binary, catalog_size);
  free(catalog_binary);
  fclose(f);
  EXPECT_TRUE(retval);

  WritableCatalog *catalog = WritableCatalog::AttachFreely("", temp_path,
                                                           shash::Any());
  unlink(temp_path.c_str());
  EXPECT_TRUE(catalog != NULL);

  shash::Any h;
  uint64_t s;
  EXPECT_FLOAT_EQ(1.0, catalog->schema());
  EXPECT_EQ(1U, catalog->ListOwnNestedCatalogs().size());
  EXPECT_EQ(1U, catalog->ListNestedCatalogs().size());
  EXPECT_TRUE(catalog->FindNested(PathString("/path"), &h, &s));
  shash::Any hash_compare = shash::MkFromHexPtr(
      shash::HexPtr("0000000000000000000000000000000000000042"));
  EXPECT_EQ(h, hash_compare);
}

}  // namespace catalog
