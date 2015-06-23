/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <sys/stat.h>
#include <unistd.h>

#include "../../cvmfs/catalog.h"
#include "../../cvmfs/catalog_rw.h"
#include "../../cvmfs/hash.h"
#include "../../cvmfs/shortstring.h"
#include "../../cvmfs/util.h"
#include "testutil.h"

using namespace std;  // NOLINT

namespace catalog {

class T_Catalog : public ::testing::Test {
 protected:
  virtual void SetUp() {
    catalog = NULL;
    sandbox = CreateTempDir("/tmp/cvmfs_ut_catalog");
    ASSERT_FALSE(sandbox.empty());

    // creating firstly the father
    string root_path = "";
    catalog_db_root = CreateCatalogDB(root_path);
    catalog::WritableCatalog *writable_root_catalog =
        catalog::WritableCatalog::AttachFreely(root_path,
                                               catalog_db_root,
                                               shash::Any(shash::kSha1),
                                               NULL,
                                               false);
    AddEntry(writable_root_catalog, "foo", "", S_IFREG,
             "988881adc9fc3655077dc2d4d757d480b5ea0e11");
    AddEntry(writable_root_catalog, "dir", "", S_IFDIR, "");
    AddEntry(writable_root_catalog, "dir", "/dir", S_IFDIR, "");
    AddEntry(writable_root_catalog, "folder", "/dir", S_IFDIR, "");
    AddEntry(writable_root_catalog, "bar", "/dir/dir", S_IFREG,
             "448fa8e3d2b1a80d4f38727cd9a85eb2c0faf433");
    AddEntry(writable_root_catalog, "bar2", "/dir/dir", S_IFREG,
             "1e12aecf3c6b0e9208cf5a22e3d5dec7edbd577e");

    // secondly the child
    shash::Any child_hash = shash::Any(shash::kSha1);
    nested_path = "/dir/folder";
    catalog_db_nested = CreateCatalogDB(nested_path);
    catalog::WritableCatalog *writable_nested_catalog =
        catalog::WritableCatalog::AttachFreely(nested_path,
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
    writable_root_catalog->InsertNestedCatalog(nested_path,
                                               NULL,
                                               child_hash,
                                               size);
    writable_root_catalog->Commit();
    delete writable_root_catalog;
  }

  string CreateCatalogDB(const string &root_path) {
    string db_file = CreateTempPath(sandbox + "/catalog", 0666);
    EXPECT_FALSE(db_file.empty());

    const bool volatile_content = false;
    {
      UniquePtr<catalog::CatalogDatabase>
        new_clg_db(catalog::CatalogDatabase::Create(db_file));
      EXPECT_TRUE(new_clg_db.IsValid());
      bool retval =
          new_clg_db->InsertInitialValues(root_path, volatile_content);
      EXPECT_TRUE(retval);
    }
    return db_file;
  }

  void AddEntry(catalog::WritableCatalog *writable_catalog,
                string name,
                string parent_path,
                unsigned mode,  // S_IFREG, S_IFDIR
                string checksum) {
    catalog::DirectoryEntryTestFactory::Metadata metadata;
    metadata.name       = name;
    metadata.mode       = mode | S_IRWXU;  // file permissions/mode
    metadata.uid        = 0;
    metadata.gid        = 0;
    metadata.size       = 4 * 1024;
    metadata.mtime      = IsoTimestamp2UtcTime("2015-06-19T09:10:18Z");
    metadata.symlink    = "";
    metadata.linkcount  = 1;
    metadata.has_xattrs = false;
    metadata.checksum   = shash::MkFromHexPtr(shash::HexPtr(checksum));
    string complete_name = parent_path + "/" + name;
    DirectoryEntry de(DirectoryEntryTestFactory::Make(metadata));
    XattrList xattrlist;
    writable_catalog->AddEntry(de, xattrlist, complete_name, parent_path);
    writable_catalog->UpdateLastModified();
  }

  virtual void TearDown() {
    delete catalog;
    RemoveTree(sandbox);
  }

  Catalog *catalog;
  string sandbox;
  string nested_path;
  string catalog_db_root;
  string catalog_db_nested;
};


TEST_F(T_Catalog, Attach) {
  catalog = catalog::Catalog::AttachFreely("",
                                           catalog_db_root,
                                           shash::Any(),
                                           NULL,
                                           false);
  EXPECT_NE(static_cast<Catalog*>(NULL), catalog);
  EXPECT_TRUE(catalog->IsInitialized());
  EXPECT_TRUE(catalog->IsRoot());
  EXPECT_FALSE(catalog->IsWritable());
  EXPECT_FALSE(catalog->HasParent());
  shash::Any hash;
  uint64_t size;
  EXPECT_TRUE(catalog->FindNested(PathString(nested_path), &hash, &size));
  Catalog *nested = catalog::Catalog::AttachFreely(nested_path,
                                                   catalog_db_nested,
                                                   shash::Any(),
                                                   catalog,
                                                   true);
  EXPECT_TRUE(nested->IsInitialized());
  EXPECT_FALSE(nested->IsRoot());
  EXPECT_FALSE(nested->IsWritable());
  EXPECT_TRUE(nested->HasParent());
  EXPECT_EQ(NULL, catalog::Catalog::AttachFreely("", "/random/path",
                                                 shash::Any()));
  delete nested;
}

TEST_F(T_Catalog, ) {

}

}
