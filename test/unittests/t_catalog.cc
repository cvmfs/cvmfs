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
    nested = NULL;
    sandbox = CreateTempDir("./cvmfs_ut_catalog");
    ASSERT_FALSE(sandbox.empty());
    FileChunk file_chunk;

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
             "988881adc9fc3655077dc2d4d757d480b5ea0e11", "", true);
    writable_root_catalog->AddFileChunk("/foo", file_chunk);
    AddEntry(writable_root_catalog, "dir", "", S_IFDIR, "");
    AddEntry(writable_root_catalog, "dir", "/dir", S_IFDIR, "");
    AddEntry(writable_root_catalog, "folder", "/dir", S_IFDIR, "");
    AddEntry(writable_root_catalog, "bar", "/dir/dir", S_IFREG,
             "448fa8e3d2b1a80d4f38727cd9a85eb2c0faf433");
    AddEntry(writable_root_catalog, "bar2", "/dir/dir", S_IFREG,
             "1e12aecf3c6b0e9208cf5a22e3d5dec7edbd577e");
    AddEntry(writable_root_catalog, "link", "/dir/dir", S_IFLNK,
             "", "/foo");

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
      bool retval = new_clg_db->InsertInitialValues(
        root_path, volatile_content, "");
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
                bool   is_chunked_file = false) {
    catalog::DirectoryEntryTestFactory::Metadata metadata;
    metadata.name       = name;
    metadata.mode       = mode | S_IRWXU;  // file permissions/mode
    metadata.uid        = getuid();
    metadata.gid        = getgid();
    metadata.size       = 4 * 1024;
    metadata.mtime      = IsoTimestamp2UtcTime("2015-06-19T09:10:18Z");
    metadata.symlink    = symlink;
    metadata.linkcount  = 1;
    metadata.has_xattrs = false;
    metadata.checksum   = shash::MkFromHexPtr(shash::HexPtr(checksum));
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
  EXPECT_EQ(NULL, catalog->parent());
  EXPECT_EQ(0u, catalog->revision());
  EXPECT_FALSE(catalog->volatile_flag());
  EXPECT_EQ(shash::Any(), catalog->hash());
  EXPECT_EQ(shash::Any(), catalog->GetPreviousRevision());
  EXPECT_EQ(catalog_db_root, catalog->database_path());
  EXPECT_NE(0u, catalog->GetTTL());
  EXPECT_EQ(7u, catalog->GetNumEntries());
  EXPECT_EQ(7u, catalog->max_row_id());

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
  nested = catalog::Catalog::AttachFreely(nested_path,
                                          catalog_db_nested,
                                          shash::Any(),
                                          catalog,
                                          true);
  EXPECT_TRUE(nested->IsInitialized());
  EXPECT_FALSE(nested->IsRoot());
  EXPECT_FALSE(nested->IsWritable());
  EXPECT_TRUE(nested->HasParent());
  EXPECT_EQ(catalog, nested->parent());
  EXPECT_EQ(2u, nested->GetNumEntries());
  EXPECT_EQ(NULL, catalog::Catalog::AttachFreely("", "/random/path",
                                                 shash::Any()));
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
  PathString link_path("/dir/dir/link");
  DirectoryEntry dirent;
  shash::Md5 md5_hash;
  XattrList xattr_list;
  EXPECT_DEATH(catalog->LookupPath(fake_path, NULL), ".*");
  EXPECT_DEATH(catalog->LookupXattrsPath(fake_path, &xattr_list), ".*");

  delete catalog;
  catalog = catalog::Catalog::AttachFreely("",
                                           catalog_db_root,
                                           shash::Any(),
                                           NULL,
                                           false);
  EXPECT_FALSE(catalog->LookupPath(fake_path, &dirent));
  EXPECT_TRUE(catalog->LookupPath(path, &dirent));
  EXPECT_TRUE(dirent.IsDirectory());
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
}

TEST_F(T_Catalog, Listing) {
  StatEntryList stat_entry_list;
  DirectoryEntryList dir_entry_list;
  PathString fake_path("/fakepath/fakefile");
  PathString path("/dir/dir");
  catalog = catalog::Catalog::AttachFreely("",
                                           catalog_db_root,
                                           shash::Any(),
                                           NULL,
                                           false);

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
  EXPECT_EQ(PathString("/dir/folder"), nc_list.at(0).path);

  FileChunkList file_chunk_list;
  EXPECT_TRUE(catalog->ListPathChunks(PathString("/foo"),
                                      shash::kSha1,
                                      &file_chunk_list));
  DirectoryEntry chunk_dir_entry;
  ASSERT_EQ(1u, file_chunk_list.size());
  ASSERT_TRUE(catalog->LookupPath(PathString("/foo"), &chunk_dir_entry));
  EXPECT_TRUE(chunk_dir_entry.IsChunkedFile());
  EXPECT_EQ(0u, file_chunk_list.AtPtr(0)->size());
  EXPECT_EQ(0u, file_chunk_list.AtPtr(0)->offset());
}

TEST_F(T_Catalog, Chunks) {
  catalog = catalog::Catalog::AttachFreely("",
                                           catalog_db_root,
                                           shash::Any(),
                                           NULL,
                                           false);
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

}  // namespace catalog
