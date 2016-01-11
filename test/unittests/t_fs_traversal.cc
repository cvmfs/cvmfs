/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <errno.h>
#include <ftw.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <string>

#include "../../cvmfs/fs_traversal.h"
#include "../../cvmfs/platform.h"
#include "../../cvmfs/util.h"

class T_FsTraversal : public ::testing::Test {
 public:
  struct Checklist {
    enum Type {
      RootDirectory,
      NonTraversedDirectory,
      Directory,
      File,
      Symlink,
      Untouched,
      Unspecified,
      Socket,
      BlockDevice,
      CharacterDevice,
      FIFO
    };

    Checklist() : type(Unspecified) {
      Init();
    }
    Checklist(const std::string &path, Type type) : type(type), path(path) {
      Init();
    }

    void Init() {
      enter_dir       = false;
      leave_dir       = false;
      file_found      = false;
      symlink_found   = false;
      dir_prefix      = false;
      dir_postfix     = false;
      socket_found    = false;
      block_dev_found = false;
      chr_dev_found   = false;
      fifo_found      = false;
    }

    void Check(const Type overwrite_type = Unspecified) const {
      const Type type_to_check =
        (overwrite_type != Unspecified) ? overwrite_type
                                        : type;

      switch (type_to_check) {
        case Directory:
          EXPECT_TRUE(enter_dir)       << path;
          EXPECT_TRUE(leave_dir)       << path;
          EXPECT_TRUE(dir_prefix)      << path;
          EXPECT_TRUE(dir_postfix)     << path;
          break;
        case RootDirectory:
          EXPECT_TRUE(enter_dir)       << path;
          EXPECT_TRUE(leave_dir)       << path;
          EXPECT_FALSE(dir_prefix)     << path;
          EXPECT_FALSE(dir_postfix)    << path;
          break;
        case NonTraversedDirectory:
          EXPECT_TRUE(dir_prefix)      << path;
          EXPECT_TRUE(dir_postfix)     << path;
          EXPECT_FALSE(enter_dir)      << path;
          EXPECT_FALSE(leave_dir)      << path;
          break;
        case File:
          EXPECT_TRUE(file_found)      << path;
          break;
        case Symlink:
          EXPECT_TRUE(symlink_found)   << path;
          break;
        case Untouched:
          EXPECT_FALSE(enter_dir)      << path;
          EXPECT_FALSE(leave_dir)      << path;
          EXPECT_FALSE(file_found)     << path;
          EXPECT_FALSE(symlink_found)  << path;
          EXPECT_FALSE(dir_prefix)     << path;
          EXPECT_FALSE(dir_postfix)    << path;
          break;
        case Socket:
          EXPECT_TRUE(socket_found)    << path;
          break;
        case BlockDevice:
          EXPECT_TRUE(block_dev_found) << path;
          break;
        case CharacterDevice:
          EXPECT_TRUE(chr_dev_found)   << path;
          break;
        case FIFO:
          EXPECT_TRUE(fifo_found)      << path;
          break;
        default:
          FAIL() << "Encountered an unexpected file type";
      }
    }

    Type        type;
    std::string path;

    // callback flags
    bool enter_dir;
    bool leave_dir;
    bool file_found;
    bool symlink_found;
    bool dir_prefix;
    bool dir_postfix;
    bool socket_found;
    bool block_dev_found;
    bool chr_dev_found;
    bool fifo_found;
  };

  typedef std::map<std::string, Checklist> ChecklistMap;

 protected:
  T_FsTraversal() : tmp_path_(".") {}

  virtual void SetUp() {
    // create a testbed directory
    const std::string path = tmp_path_ + "/cvmfs_T_FsTraversal_testbed_XXXXXX";
    char *tmp_file_path = strdupa(path.c_str());
    char *testbed_path = mkdtemp(tmp_file_path);
    ASSERT_NE(static_cast<char*>(NULL), testbed_path)
      << "can't create testbed from '" << tmp_file_path << "' "
      << "(errno: " << errno << ")";
    testbed_path_ = std::string(testbed_path);
    ASSERT_FALSE(testbed_path_.empty());

    // save the root entry (the testbed) into the reference list
    reference_[""] = Checklist("", Checklist::RootDirectory);

    // define a reference directory structure
    GenerateReferenceDirectoryStructure();
  }

  virtual void TearDown() {
    // cleaning up the test directory
    int retval = nftw(testbed_path_.c_str(),
                      &T_FsTraversal::delete_entry,
                      50,
                      FTW_DEPTH | FTW_PHYS);
    EXPECT_EQ(0, retval) << "Failed to delete testbed directory "
                         << "'" << testbed_path_ << "' "
                         << "(errno: " << errno << ")";
  }

  static int delete_entry(const char         *path,
                          const struct stat  *stat_data,
                          int                 flag,
                          struct FTW         *ftw) {
    int retval = -1;

    switch (flag) {
      case FTW_DP:
        retval = rmdir(path);
        break;
      case FTW_F:
      case FTW_SL:
        retval = unlink(path);
        break;
      default:
        fail_entry_flag(flag);
    }

    return retval;
  }

  // Small Hack: FAIL() is only allowed in functions returning 'void'
  static void fail_entry_flag(const int flag) {
    FAIL() << "unexpected directory entry type: " << flag;
  }


  template<class DelegateT>
  void RegisterDelegate(FileSystemTraversal<DelegateT> *traverse) {
    traverse->fn_enter_dir       = &DelegateT::EnterDir;
    traverse->fn_leave_dir       = &DelegateT::LeaveDir;
    traverse->fn_new_file        = &DelegateT::File;
    traverse->fn_new_symlink     = &DelegateT::Symlink;
    traverse->fn_new_dir_prefix  = &DelegateT::DirPrefix;
    traverse->fn_new_dir_postfix = &DelegateT::DirPostfix;
    traverse->fn_new_socket      = &DelegateT::Socket;
    traverse->fn_new_block_dev   = &DelegateT::BlockDevice;
    traverse->fn_new_fifo        = &DelegateT::Fifo;
  }


 private:
  void MakeDirectory(const std::string &relative_path) {
    const std::string path = testbed_path_ + "/" + relative_path;
    const int retval = mkdir(path.c_str(), 0755);
    ASSERT_EQ(0, retval) << path << "errno: " << errno;
    reference_[relative_path] = Checklist(relative_path, Checklist::Directory);
  }

  void MakeFile(const std::string &relative_path) {
    const std::string path = testbed_path_ + "/" + relative_path;
    FILE *file = fopen(path.c_str(), "w+");
    ASSERT_NE(static_cast<FILE*>(NULL), file);
    const int retval = fclose(file);
    ASSERT_EQ(0, retval);
    reference_[relative_path] = Checklist(relative_path, Checklist::File);
  }

  void MakeSymlink(const std::string &relative_path,
                   const std::string &link_destination) {
    const std::string path = testbed_path_ + "/" + relative_path;
    const int retval = symlink(link_destination.c_str(), path.c_str());
    ASSERT_EQ(0, retval) << "errno: " << errno;
    reference_[relative_path] = Checklist(relative_path, Checklist::Symlink);
  }

  void CreateSocket(const std::string &relative_path) {
    const std::string path = testbed_path_ + "/" + relative_path;
    const int retval = MakeSocket(path, 0755);
    ASSERT_NE(-1, retval) << "errno: " << errno;
    reference_[relative_path] = Checklist(relative_path, Checklist::Socket);
  }

  void MakeFifo(const std::string &relative_path) {
    const std::string path = testbed_path_ + "/" + relative_path;
    const int retval = mkfifo(path.c_str(), 0755);
    ASSERT_EQ(0, retval) << "errno: " << errno;
    reference_[relative_path] = Checklist(relative_path, Checklist::FIFO);
  }

  void GenerateReferenceDirectoryStructure() {
    MakeDirectory("a");
    MakeDirectory("a/a");
    MakeFile("a/a/foo");
    MakeFile("a/a/bar");
    CreateSocket("a/a/socket1");
    CreateSocket("a/a/socket2");
    MakeFifo("a/a/fifo1");
    MakeFifo("a/a/fifo2");
    MakeDirectory("a/b");
    MakeFile("a/b/foo");
    MakeFile("a/b/bar");
    MakeDirectory("a/c");
    MakeDirectory("a/c/a");
    MakeFile("a/c/a/foo");
    MakeFile("a/c/a/bar");
    MakeFile("a/c/a/baz");
    CreateSocket("a/c/a/socket");
    MakeFifo("a/c/a/fifo");
    MakeDirectory("a/c/b");
    MakeDirectory("a/c/c");
    MakeDirectory("a/c/d");
    MakeFile("a/c/foo");
    MakeFile("a/c/bar");
    MakeFile("a/c/baz");
    MakeSymlink("a/c/lnk", "baz");
    CreateSocket("a/c/socket");
    MakeFifo("a/c/fifo");
    MakeDirectory("a/d");
    MakeDirectory("b");
    MakeDirectory("b/a");
    MakeDirectory("b/b");
    MakeDirectory("b/b/a");
    MakeDirectory("b/b/a/a");
    MakeDirectory("b/b/a/b");
    MakeDirectory("b/b/a/c");
    MakeDirectory("b/b/a/c/a");
    MakeDirectory("b/b/a/c/b");
    MakeDirectory("b/b/a/c/c");
    MakeFile("b/b/a/c/c/foo");
    MakeFile("b/b/a/c/c/bar");
    MakeFile("b/b/a/c/c/baz");
    CreateSocket("b/b/a/c/c/socket");
    MakeFifo("b/b/a/c/c/fifo");
    MakeSymlink("b/b/a/c/c/2b", "../b");
    MakeDirectory("b/b/a/c/d");
    MakeDirectory("b/b/a/c/e");
    MakeDirectory("b/b/a/d");
    MakeDirectory("b/b/a/d/a");
    MakeDirectory("b/b/a/d/b");
    MakeDirectory("b/b/a/d/c");
    MakeDirectory("b/b/a/d/d");
    MakeDirectory("b/b/a/d/e");
    MakeDirectory("b/b/b");
    MakeDirectory("b/b/b/e");
    MakeDirectory("b/b/c");
    MakeDirectory("b/c");
    MakeDirectory("b/d");
    MakeDirectory("b/e");
    MakeDirectory("c");
    MakeDirectory("c/a");
    MakeFile("c/a/foo");
    CreateSocket("c/a/socket");
    MakeFifo("c/a/fifo");
    MakeSymlink("c/a/bfoo", "../b/foo");
    MakeDirectory("c/b");
    MakeFile("c/b/foo");
    MakeDirectory("c/c");
    MakeFile("c/c/foo");
    MakeDirectory("c/d");
    MakeFile("c/d/foo");
    MakeFifo("c/d/fifo");
    MakeDirectory("c/e");
    MakeFile("c/e/foo");
    MakeDirectory("c/f");
    MakeFile("c/f/foo");
    MakeFile("c/foo");
    CreateSocket("c/socket");
    MakeFifo("c/fifo");
  }

 protected:
  const std::string tmp_path_;
  std::string       testbed_path_;

  ChecklistMap      reference_;
};


class BaseTraversalDelegate {
 public:
  typedef T_FsTraversal::Checklist    Checklist;
  typedef T_FsTraversal::ChecklistMap ChecklistMap;

 public:
  explicit BaseTraversalDelegate(const ChecklistMap &reference) :
    reference_(reference) {}

  virtual ~BaseTraversalDelegate() { }

  virtual void EnterDir(const std::string &relative_path,
                        const std::string &dir_name) {
    Checklist& checklist = GetChecklist(CombinePath(relative_path, dir_name));
    checklist.enter_dir = true;
  }

  virtual void LeaveDir(const std::string &relative_path,
                        const std::string &dir_name) {
    Checklist& checklist = GetChecklist(CombinePath(relative_path, dir_name));
    checklist.leave_dir = true;
  }

  virtual void File(const std::string &relative_path,
                    const std::string &file_name) {
    Checklist& checklist = GetChecklist(CombinePath(relative_path, file_name));
    checklist.file_found = true;
  }

  virtual void Symlink(const std::string &relative_path,
                       const std::string &link_name) {
    Checklist& checklist = GetChecklist(CombinePath(relative_path, link_name));
    checklist.symlink_found = true;
  }

  virtual bool DirPrefix(const std::string &relative_path,
                         const std::string &dir_name) {
    Checklist& checklist = GetChecklist(CombinePath(relative_path, dir_name));
    checklist.dir_prefix = true;
    return true;
  }

  virtual void DirPostfix(const std::string &relative_path,
                          const std::string &dir_name) {
    Checklist& checklist = GetChecklist(CombinePath(relative_path, dir_name));
    checklist.dir_postfix = true;
  }

  virtual void Socket(const std::string &relative_path,
      const std::string &dir_name) {
    Checklist& checklist = GetChecklist(CombinePath(relative_path, dir_name));
    checklist.socket_found = true;
  }

  virtual void Fifo(const std::string &relative_path,
        const std::string &dir_name) {
      Checklist& checklist = GetChecklist(CombinePath(relative_path, dir_name));
      checklist.fifo_found = true;
  }

  virtual void BlockDevice(const std::string &relative_path,
        const std::string &dir_name) {
      Checklist& checklist = GetChecklist(CombinePath(relative_path, dir_name));
      checklist.block_dev_found = true;
  }

  virtual void Check() const {
    ChecklistMap::const_iterator i    = reference_.begin();
    ChecklistMap::const_iterator iend = reference_.end();
    for (; i != iend; ++i) {
      i->second.Check();
    }
  }

 protected:
  inline std::string CombinePath(const std::string &relative_path,
                                 const std::string &entry_name) const {
    return (relative_path.empty()) ? entry_name
                                   : relative_path + "/" + entry_name;
  }


  inline Checklist& GetChecklist(const std::string &path) {
    return const_cast<Checklist&>(__GetChecklist(path));
  }

  inline const Checklist& GetChecklist(const std::string &path) const {
    return __GetChecklist(path);
  }


  void CheckPathes(
         const std::set<std::string> &pathes,
         const Checklist::Type        for_type = Checklist::Unspecified) const {
    std::set<std::string>::const_iterator i    = pathes.begin();
    std::set<std::string>::const_iterator iend = pathes.end();
    for (; i != iend; ++i) {
      GetChecklist(*i).Check(for_type);
    }
  }


  void CheckAllExcept(
         const std::set<std::string> &pathes,
         const Checklist::Type        for_type = Checklist::Unspecified) const {
    ChecklistMap::const_iterator i    = reference_.begin();
    ChecklistMap::const_iterator iend = reference_.end();
    for (; i != iend; ++i) {
      if (pathes.find(i->first) == pathes.end()) {
        i->second.Check(for_type);
      }
    }
  }


  void fail(const std::string msg) const {
    FAIL() << msg;
  }

 private:
  inline const Checklist& __GetChecklist(const std::string &path) const {
    ChecklistMap::const_iterator checklist = reference_.find(path);
    if (reference_.end() == checklist) {
      fail("Did not find traversed path '" + path + "'");
    }
    return checklist->second;
  }

 protected:
  T_FsTraversal::ChecklistMap reference_;
};


//
// # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
//

TEST_F(T_FsTraversal, FullTraversal) {
  BaseTraversalDelegate delegate(reference_);
  FileSystemTraversal<BaseTraversalDelegate> traverse(&delegate,
                                                       testbed_path_,
                                                       true);
  RegisterDelegate(&traverse);

  traverse.Recurse(testbed_path_);
  delegate.Check();
}


//
// # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
//


class RootTraversalDelegate : public BaseTraversalDelegate {
 public:
  explicit RootTraversalDelegate(const ChecklistMap &reference) :
    BaseTraversalDelegate(reference) {}

  void Check() const {
    // check touched entries
    GetChecklist("").Check();
    GetChecklist("a").Check(Checklist::NonTraversedDirectory);
    GetChecklist("b").Check(Checklist::NonTraversedDirectory);
    GetChecklist("c").Check(Checklist::NonTraversedDirectory);

    // check untouched entries
    std::set<std::string> touched_pathes;
    touched_pathes.insert("");
    touched_pathes.insert("a");
    touched_pathes.insert("b");
    touched_pathes.insert("c");
    CheckAllExcept(touched_pathes, Checklist::Untouched);
  }
};

TEST_F(T_FsTraversal, RootTraversal) {
  RootTraversalDelegate delegate(reference_);
  FileSystemTraversal<RootTraversalDelegate> traverse(&delegate,
                                                       testbed_path_,
                                                       false);
  RegisterDelegate(&traverse);

  traverse.Recurse(testbed_path_);
  delegate.Check();
}


//
// # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
//


class IgnoringTraversalDelegate : public BaseTraversalDelegate {
 public:
  explicit IgnoringTraversalDelegate(const ChecklistMap &reference) :
    BaseTraversalDelegate(reference) {}

  void Check() const {
    std::set<std::string> ignored_pathes;
    ignored_pathes.insert("a/c/a/baz");
    ignored_pathes.insert("a/c/baz");
    ignored_pathes.insert("b/b/a/c/c/baz");
    ignored_pathes.insert("b/b/a/c/c/fifo");
    ignored_pathes.insert("b/b/a/c/c/socket");
    ignored_pathes.insert("a/d");
    ignored_pathes.insert("a/c/d");
    ignored_pathes.insert("b/b/a/c/d");
    ignored_pathes.insert("b/b/a/d");
    ignored_pathes.insert("b/b/a/d/a");
    ignored_pathes.insert("b/b/a/d/b");
    ignored_pathes.insert("b/b/a/d/c");
    ignored_pathes.insert("b/b/a/d/d");
    ignored_pathes.insert("b/b/a/d/e");
    ignored_pathes.insert("b/d");
    ignored_pathes.insert("c/d");
    ignored_pathes.insert("c/d/fifo");
    ignored_pathes.insert("c/d/foo");

    CheckAllExcept(ignored_pathes);
    CheckPathes(ignored_pathes, Checklist::Untouched);
  }

  void SetIgnoreNames(const std::set<std::string> &ignore_names) {
    ignore_names_ = ignore_names;
  }

  bool IgnoreFilePredicate(const std::string &parent_dir,
                           const std::string &filename)
  {
    return (ignore_names_.find(filename) != ignore_names_.end());
  }

 private:
  std::set<std::string> ignore_names_;
};

TEST_F(T_FsTraversal, IgnoringTraversal) {
  // ignore all files call "baz"
  std::set<std::string> ignored_filenames;
  ignored_filenames.insert("baz");
  ignored_filenames.insert("d");

  IgnoringTraversalDelegate delegate(reference_);
  delegate.SetIgnoreNames(ignored_filenames);
  FileSystemTraversal<IgnoringTraversalDelegate> traverse(&delegate,
                                                           testbed_path_,
                                                           true);
  RegisterDelegate(&traverse);
  traverse.fn_ignore_file = &IgnoringTraversalDelegate::IgnoreFilePredicate;

  traverse.Recurse(testbed_path_);
  delegate.Check();
}


//
// # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
//


class SteeringTraversalDelegate : public BaseTraversalDelegate {
 public:
  explicit SteeringTraversalDelegate(const ChecklistMap &reference) :
    BaseTraversalDelegate(reference) {}


  virtual bool DirPrefix(const std::string &relative_path,
                         const std::string &dir_name) {
    BaseTraversalDelegate::DirPrefix(relative_path, dir_name);

    const std::string path = CombinePath(relative_path, dir_name);
    if (path == "a/c/a" || path == "b/b/a/c") {
      return false;
    } else {
      return true;
    }
  }


  void Check() const {
    std::set<std::string> fully_ignored_pathes;
    fully_ignored_pathes.insert("a/c/a/foo");
    fully_ignored_pathes.insert("a/c/a/bar");
    fully_ignored_pathes.insert("a/c/a/baz");
    fully_ignored_pathes.insert("a/c/a/fifo");
    fully_ignored_pathes.insert("a/c/a/socket");

    fully_ignored_pathes.insert("b/b/a/c/a");
    fully_ignored_pathes.insert("b/b/a/c/b");
    fully_ignored_pathes.insert("b/b/a/c/c");
    fully_ignored_pathes.insert("b/b/a/c/c/foo");
    fully_ignored_pathes.insert("b/b/a/c/c/bar");
    fully_ignored_pathes.insert("b/b/a/c/c/baz");
    fully_ignored_pathes.insert("b/b/a/c/c/fifo");
    fully_ignored_pathes.insert("b/b/a/c/c/socket");
    fully_ignored_pathes.insert("b/b/a/c/c/2b");
    fully_ignored_pathes.insert("b/b/a/c/d");
    fully_ignored_pathes.insert("b/b/a/c/e");

    std::set<std::string> seen_but_non_traversed_dirs;
    seen_but_non_traversed_dirs.insert("a/c/a");
    seen_but_non_traversed_dirs.insert("b/b/a/c");

    std::set<std::string> all_special_cases;
    all_special_cases.insert(fully_ignored_pathes.begin(),
                             fully_ignored_pathes.end());
    all_special_cases.insert(seen_but_non_traversed_dirs.begin(),
                             seen_but_non_traversed_dirs.end());

    CheckAllExcept(all_special_cases);
    CheckPathes(fully_ignored_pathes, Checklist::Untouched);
    CheckPathes(seen_but_non_traversed_dirs, Checklist::NonTraversedDirectory);
  }
};

TEST_F(T_FsTraversal, SteeredTraversal) {
  SteeringTraversalDelegate delegate(reference_);
  FileSystemTraversal<SteeringTraversalDelegate> traverse(&delegate,
                                                           testbed_path_,
                                                           true);
  RegisterDelegate(&traverse);

  traverse.Recurse(testbed_path_);
  delegate.Check();
}

class CustomDelegate {
 public:
  explicit CustomDelegate(const std::string &path) :
    num_block_dev(0), num_character_dev(0), root_path(path) {}

  void BlockDevice(const std::string &relative_path,
                   const std::string &object_name) {
    platform_stat64 s;
    GetStat(relative_path, object_name, &s);
    EXPECT_TRUE(S_ISBLK(s.st_mode));
    ++num_block_dev;
  }

  void CharacterDevice(const std::string &relative_path,
                       const std::string &object_name) {
    platform_stat64 s;
    GetStat(relative_path, object_name, &s);
    EXPECT_TRUE(S_ISCHR(s.st_mode));
    ++num_character_dev;
  }

  bool CheckPermissions(const std::string &relative_path,
                        const std::string &object_name) {
    platform_stat64 s;
    GetStat(relative_path, object_name, &s);
    const bool can_read =  (S_ISDIR(s.st_mode) && s.st_mode & S_IRUSR &&
                                                  s.st_mode & S_IXUSR)
                       || (!S_ISDIR(s.st_mode) && s.st_mode & S_IRUSR);
    return !can_read;
  }

 protected:
  void GetStat(const std::string  &relative_path,
               const std::string  &obj_name,
               platform_stat64    *s) const {
    const std::string file = root_path + "/" + relative_path + "/" + obj_name;
    const int retval = platform_lstat(file.c_str(), s);
    ASSERT_EQ(0, retval) << "cannot stat '" << file << "' (" << errno << ")";
  }

 public:
  int                num_block_dev;
  int                num_character_dev;
  const std::string  root_path;
};

TEST_F(T_FsTraversal, BlockDevice) {
  CustomDelegate delegate("/dev");
  FileSystemTraversal<CustomDelegate> traverse(&delegate,
                                                "/dev",
                                                true);
  traverse.fn_new_block_dev = &CustomDelegate::BlockDevice;
  traverse.fn_ignore_file = &CustomDelegate::CheckPermissions;
  traverse.Recurse("/dev");
  EXPECT_LT(0, delegate.num_block_dev);
}

TEST_F(T_FsTraversal, CharacterDevice) {
  CustomDelegate delegate("/dev");
  FileSystemTraversal<CustomDelegate> traverse(&delegate,
                                                "/dev",
                                                true);
  traverse.fn_new_character_dev = &CustomDelegate::CharacterDevice;
  traverse.fn_ignore_file       = &CustomDelegate::CheckPermissions;
  traverse.Recurse("/dev");
  EXPECT_LT(0, delegate.num_character_dev);
}
