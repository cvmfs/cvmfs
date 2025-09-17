package main

import (
	"fmt"
	"os"
	"os/user"
	"strconv"
	"strings"
	"testing"
	"time"

	pathlib "github.com/chigopher/pathlib"
	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/pkg"
)

type PathNameMode struct {
	PathString string
	Mode       int
}

func setupFileTestsE2E(t *testing.T) (func(t *testing.T), string, string, string, string, string, string) {
	srcDir, err := os.MkdirTemp(pkg.TestingTempDir(), "cvmfs_test_src_dir")
	if err != nil {
		panic(err)
	}
	f, err := os.CreateTemp(srcDir, "test_file.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f.Write([]byte{1, 2, 3, 4}); err != nil {
		panic(err)
	}
	if err := f.Close(); err != nil {
		panic(err)
	}
	updatef, err := os.CreateTemp(srcDir, "test_file_update.txt")
	if err != nil {
		panic(err)
	}
	if _, err := updatef.Write([]byte{4, 3, 2, 1, 0}); err != nil {
		panic(err)
	}
	if err := updatef.Close(); err != nil {
		panic(err)
	}
	fDot, err := os.Create(srcDir + "/test_file.txt.")
	if err != nil {
		panic(err)
	}
	if _, err := fDot.Write([]byte{1, 2, 3, 4}); err != nil {
		panic(err)
	}
	if err := fDot.Close(); err != nil {
		panic(err)
	}
	fDotDot, err := os.Create(srcDir + "/test_file.txt..")
	if err != nil {
		panic(err)
	}
	if _, err := fDotDot.Write([]byte{1, 2, 3, 4}); err != nil {
		panic(err)
	}
	if err := fDotDot.Close(); err != nil {
		panic(err)
	}
	fCatalog, err := os.Create(srcDir + "/.cvmfscatalog")
	if err != nil {
		panic(err)
	}
	if _, err := fCatalog.Write([]byte{1, 2, 3, 4}); err != nil {
		panic(err)
	}
	if err := fCatalog.Close(); err != nil {
		panic(err)
	}
	hostDir, err := os.MkdirTemp(srcDir, "host_dir")
	if err != nil {
		panic(err)
	}
	fInner, err := os.Create(hostDir + "/test_file.txt")
	if err != nil {
		panic(err)
	}
	if _, err := fInner.Write([]byte{1, 2, 3, 4}); err != nil {
		panic(err)
	}
	if err := fInner.Close(); err != nil {
		panic(err)
	}

	return func(t *testing.T) {
		os.RemoveAll(srcDir)
	}, f.Name(), updatef.Name(), fDot.Name(), fDotDot.Name(), fCatalog.Name(), fInner.Name()
}

func setupDirTestE2E(t *testing.T) (func(t *testing.T), string, string, string, string, string, string) {
	srcDir, err := os.MkdirTemp(pkg.TestingTempDir(), "cvmfs_test_src_dir")
	if err != nil {
		panic(err)
	}
	emptyDir, err := os.MkdirTemp(srcDir, "empty_dir")
	if err != nil {
		panic(err)
	}
	f, err := os.CreateTemp(srcDir, "test_file.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f.Write([]byte{1, 2, 3, 4}); err != nil {
		panic(err)
	}
	if err := f.Close(); err != nil {
		panic(err)
	}
	trailingDot := srcDir + "/.trailing."
	err = os.Mkdir(trailingDot, 493)
	if err != nil {
		panic(err)
	}
	trailingDotDot := srcDir + "/.trailing.."
	err = os.Mkdir(trailingDotDot, 493)
	if err != nil {
		panic(err)
	}
	hostDir, err := os.MkdirTemp(srcDir, "host_dir")
	if err != nil {
		panic(err)
	}
	cvmfsCatalog := hostDir + "/.cvmfscatalog"
	err = os.Mkdir(cvmfsCatalog, 493)
	if err != nil {
		panic(err)
	}
	hostDir2, err := os.MkdirTemp(srcDir, "host_dir_2")
	if err != nil {
		panic(err)
	}
	hostInnerDir, err := os.MkdirTemp(hostDir2, "empty_dir")
	if err != nil {
		panic(err)
	}

	return func(t *testing.T) {
		os.RemoveAll(srcDir)
	}, f.Name(), emptyDir, trailingDot, trailingDotDot, cvmfsCatalog, hostInnerDir
}

func setupSymTestE2E(t *testing.T) (func(t *testing.T), string, string, string, string, string, string, string, string, string, string, string, string, string) {
	srcDir, err := os.MkdirTemp(pkg.TestingTempDir(), "cvmfs_test_src_dir")
	if err != nil {
		panic(err)
	}
	emptyDir, err := os.MkdirTemp(srcDir, "empty_dir")
	if err != nil {
		panic(err)
	}
	f, err := os.CreateTemp(srcDir, "test_file.txt")
	if err != nil {
		panic(err)
	}
	symDir := srcDir + "/empty_dir_sym"
	emptyDirPath := pathlib.NewPath(emptyDir)
	fPath := pathlib.NewPath(f.Name())
	os.Symlink(emptyDirPath.Name(), symDir)
	symFile := srcDir + "/test_file_sym.txt"
	os.Symlink(fPath.Name(), symFile)
	brokenSymFile := srcDir + "/broken_sym.txt"
	os.Symlink("Broken", brokenSymFile)
	if _, err := f.Write([]byte{1, 2, 3, 4}); err != nil {
		panic(err)
	}
	if err := f.Close(); err != nil {
		panic(err)
	}

	sameLinkDir := srcDir + "/SameLinkName"
	err = os.Mkdir(sameLinkDir, 493)
	if err != nil {
		panic(err)
	}
	err = os.Mkdir(sameLinkDir+"/inner_dir", 493)
	if err != nil {
		panic(err)
	}
	sameFile, err := os.Create(sameLinkDir + "/inner_dir/same_name")
	if err != nil {
		panic(err)
	}
	if _, err := sameFile.Write([]byte{1, 2, 3, 4, 5, 6}); err != nil {
		panic(err)
	}
	if err := sameFile.Close(); err != nil {
		panic(err)
	}
	os.Symlink("inner_dir/same_name", sameLinkDir+"/same_name")

	sameLinkDirOver := srcDir + "/SameLinkNameOver"
	err = os.Mkdir(sameLinkDirOver, 493)
	if err != nil {
		panic(err)
	}
	err = os.Mkdir(sameLinkDirOver+"/inner_dir", 493)
	if err != nil {
		panic(err)
	}
	sameFileChange, err := os.Create(sameLinkDirOver + "/inner_dir/same_name")
	if err != nil {
		panic(err)
	}
	if _, err := sameFileChange.Write([]byte{1, 2, 3, 4, 5, 6, 7, 8}); err != nil {
		panic(err)
	}
	if err := sameFileChange.Close(); err != nil {
		panic(err)
	}
	os.Symlink("inner_dir/same_name", sameLinkDirOver+"/same_name")
	selfSymlink := srcDir + "/self_sym"
	os.Symlink("self_sym", selfSymlink)

	trailingDot := srcDir + "/.sym_trailing."
	os.Symlink("tri", trailingDot)
	trailingDotDot := srcDir + "/.sym_trailing.."
	os.Symlink("ra", trailingDotDot)
	hostDirSym, err := os.MkdirTemp(srcDir, "host_dir_sym")
	if err != nil {
		panic(err)
	}
	cvmfsCatalog := hostDirSym + "/.cvmfscatalog"
	os.Symlink("rmba", cvmfsCatalog)
	hostInnerDir, err := os.MkdirTemp(srcDir, "host_dir_sym_dot_dot")
	if err != nil {
		panic(err)
	}
	innerSymDir := hostInnerDir + "/empty_sym"
	os.Symlink(emptyDir, innerSymDir)
	innerSymFile := hostInnerDir + "/file_sym"
	os.Symlink(f.Name(), innerSymFile)

	return func(t *testing.T) {
		os.RemoveAll(srcDir)
	}, emptyDir, f.Name(), symDir, symFile, sameLinkDir, sameLinkDirOver, brokenSymFile, selfSymlink, trailingDot, trailingDotDot, cvmfsCatalog, innerSymFile, innerSymDir
}

func setupDirRecursiveTestE2E(t *testing.T) (func(t *testing.T), string, string, string, string, string, string, string, string, string, string) {
	srcDir, err := os.MkdirTemp(pkg.TestingTempDir(), "cvmfs_test_src_dir")
	if err != nil {
		panic(err)
	}
	fullDir, err := os.MkdirTemp(srcDir, "full_dir")
	if err != nil {
		panic(err)
	}
	innerDir, err := os.MkdirTemp(fullDir, "inner_dir")
	if err != nil {
		panic(err)
	}
	innerEmptyDir, err := os.MkdirTemp(fullDir, "inner_empty_dir")
	if err != nil {
		panic(err)
	}
	f, err := os.CreateTemp(fullDir, "test_file.txt")
	if err != nil {
		panic(err)
	}
	innerf, err := os.CreateTemp(innerDir, "inner_test_file.txt")
	if err != nil {
		panic(err)
	}
	innerDirPath := pathlib.NewPath(innerDir)
	innerEmptyDirPath := pathlib.NewPath(innerEmptyDir)
	fPath := pathlib.NewPath(f.Name())
	innerfPath := pathlib.NewPath(innerf.Name())
	os.Symlink(innerDirPath.Name(), fullDir+"/inner_dir_sym")
	os.Symlink(innerfPath.Name(), innerDir+"/inner_test_file_sym.txt")
	if _, err := f.Write([]byte{1, 2, 3, 4}); err != nil {
		panic(err)
	}
	if err := f.Close(); err != nil {
		panic(err)
	}
	if _, err := innerf.Write([]byte{4, 3, 2, 1}); err != nil {
		panic(err)
	}
	if err := innerf.Close(); err != nil {
		panic(err)
	}
	// Setup dir to recursively test with
	fullDirUpdate, err := os.MkdirTemp(srcDir, "full_dir_copy")
	if err != nil {
		panic(err)
	}
	err = os.Mkdir(fullDirUpdate+"/"+innerDirPath.Name(), 0755)
	if err != nil {
		panic(err)
	}
	err = os.Mkdir(fullDirUpdate+"/"+innerEmptyDirPath.Name(), 0755)
	if err != nil {
		panic(err)
	}
	fUpdate, err := os.Create(fullDirUpdate + "/" + fPath.Name())
	if err != nil {
		panic(err)
	}
	innerfUpdate, err := os.Create(fullDirUpdate + "/" + innerDirPath.Name() + "/" + innerfPath.Name())
	if err != nil {
		panic(err)
	}
	innerfNewFile, err := os.Create(fullDirUpdate + "/" + innerDirPath.Name() + "/new_file.txt")
	if err != nil {
		panic(err)
	}
	innerEmptyDirUpdatePath := pathlib.NewPath(fullDirUpdate + "/" + innerEmptyDirPath.Name())
	os.Symlink(innerEmptyDirUpdatePath.Name(), fullDirUpdate+"/inner_dir_sym")
	os.Symlink(innerEmptyDirUpdatePath.Name(), fullDirUpdate+"/"+innerDirPath.Name()+"/inner_test_file_sym.txt")
	if _, err := fUpdate.Write([]byte{1, 1, 1}); err != nil {
		panic(err)
	}
	if err := fUpdate.Close(); err != nil {
		panic(err)
	}
	if _, err := innerfUpdate.Write([]byte{4, 4, 4}); err != nil {
		panic(err)
	}
	if err := innerfUpdate.Close(); err != nil {
		panic(err)
	}
	if _, err := innerfNewFile.Write([]byte{5, 5, 5, 5}); err != nil {
		panic(err)
	}
	if err := innerfNewFile.Close(); err != nil {
		panic(err)
	}

	fullDirReplace, err := os.MkdirTemp(srcDir, "full_dir_replace")
	if err != nil {
		panic(err)
	}
	err = os.Mkdir(fullDirReplace+"/"+innerDirPath.Name(), 0755)
	if err != nil {
		panic(err)
	}
	err = os.Mkdir(fullDirReplace+"/"+fPath.Name(), 0755)
	if err != nil {
		panic(err)
	}
	err = os.Mkdir(fullDirReplace+"/inner_dir_sym", 0755)
	if err != nil {
		panic(err)
	}
	fOverSym, err := os.Create(fullDirReplace + "/" + innerDirPath.Name() + "/inner_test_file_sym.txt")
	if err != nil {
		panic(err)
	}
	if _, err := fOverSym.Write([]byte{2, 3, 4}); err != nil {
		panic(err)
	}
	if err := fOverSym.Close(); err != nil {
		panic(err)
	}
	fOverDir, err := os.Create(fullDirReplace + "/" + innerEmptyDirPath.Name())
	if err != nil {
		panic(err)
	}
	if _, err := fOverDir.Write([]byte{4, 5, 6}); err != nil {
		panic(err)
	}
	if err := fOverDir.Close(); err != nil {
		panic(err)
	}

	fullDirErrReplace, err := os.MkdirTemp(srcDir, "full_dir_err_replace")
	if err != nil {
		panic(err)
	}
	fOverFullDir, err := os.Create(fullDirErrReplace + "/" + innerDirPath.Name())
	if err != nil {
		panic(err)
	}
	if _, err := fOverFullDir.Write([]byte{1, 2, 3, 4, 5}); err != nil {
		panic(err)
	}
	if err := fOverFullDir.Close(); err != nil {
		panic(err)
	}

	fullDirErrSymReplace, err := os.MkdirTemp(srcDir, "full_dir_err_replace")
	if err != nil {
		panic(err)
	}
	fOverDirPath := pathlib.NewPath(fOverDir.Name())

	// These link targets don't really matter at all
	os.Symlink("Burn_hands", fullDirErrSymReplace+"/"+innerDirPath.Name())
	fullDirSymReplace, err := os.MkdirTemp(srcDir, "full_dir_sym_replace")
	if err != nil {
		panic(err)
	}
	// These link targets don't really matter at all
	os.Symlink("Rays_frost", fullDirSymReplace+"/inner_dir_sym")
	os.Symlink("Plain_sit", fullDirSymReplace+"/"+innerEmptyDirPath.Name())
	os.Symlink("Banner", fullDirSymReplace+"/"+fOverDirPath.Name())
	// hashData, err := hashFile(fOverDirPath)
	// os.Symlink("Baneshmint", fullDirErrSymReplace+"/."+fOverDirPath.Name()+"."+fmt.Sprintf("%040x", hashData.checksum))

	fullDirDotStuff, err := os.MkdirTemp(srcDir, "full_dir_dot_stuff")
	if err != nil {
		panic(err)
	}
	fullDirDotInnerFiles := fullDirDotStuff + "/innerFiles"
	err = os.Mkdir(fullDirDotInnerFiles, 0755)
	if err != nil {
		panic(err)
	}
	fullDirDotInnerF, err := os.Create(fullDirDotInnerFiles + "/trailing.txt.")
	if err != nil {
		panic(err)
	}
	if _, err := fullDirDotInnerF.Write([]byte{1, 2, 3, 4, 5}); err != nil {
		panic(err)
	}
	if err := fullDirDotInnerF.Close(); err != nil {
		panic(err)
	}
	fullDirDotDotInnerF, err := os.Create(fullDirDotInnerFiles + "/trailing.txt..")
	if err != nil {
		panic(err)
	}
	if _, err := fullDirDotDotInnerF.Write([]byte{1, 2, 3, 4, 5}); err != nil {
		panic(err)
	}
	if err := fullDirDotDotInnerF.Close(); err != nil {
		panic(err)
	}
	fullDirDotInnerDirs := fullDirDotStuff + "/innerDirs"
	err = os.Mkdir(fullDirDotInnerDirs, 0755)
	if err != nil {
		panic(err)
	}
	err = os.Mkdir(fullDirDotInnerDirs+"/trailing.txt.", 0755)
	if err != nil {
		panic(err)
	}
	err = os.Mkdir(fullDirDotInnerDirs+"/trailing.txt..", 0755)
	if err != nil {
		panic(err)
	}
	fullDirDotInnerSyms := fullDirDotStuff + "/innerSyms"
	err = os.Mkdir(fullDirDotInnerSyms, 0755)
	if err != nil {
		panic(err)
	}
	err = os.Symlink("acon", fullDirDotInnerSyms+"/trailing.txt.")
	if err != nil {
		panic(err)
	}
	err = os.Symlink("arc", fullDirDotInnerSyms+"/trailing.txt..")
	if err != nil {
		panic(err)
	}

	fullDirCatalogStuff, err := os.MkdirTemp(srcDir, "full_dir_catalog_stuff")
	if err != nil {
		panic(err)
	}
	fullDirCatalogInnerFiles := fullDirCatalogStuff + "/innerFiles"
	err = os.Mkdir(fullDirCatalogInnerFiles, 0755)
	if err != nil {
		panic(err)
	}
	fullDirCatalogInnerF, err := os.Create(fullDirCatalogInnerFiles + "/.cvmfscatalog")
	if err != nil {
		panic(err)
	}
	if _, err := fullDirCatalogInnerF.Write([]byte{1, 2, 3, 4, 5}); err != nil {
		panic(err)
	}
	if err := fullDirCatalogInnerF.Close(); err != nil {
		panic(err)
	}
	fullDirCatalogInnerDirs := fullDirCatalogStuff + "/innerDirs"
	err = os.Mkdir(fullDirCatalogInnerDirs, 0755)
	if err != nil {
		panic(err)
	}
	err = os.Mkdir(fullDirCatalogInnerDirs+"/.cvmfscatalog", 0755)
	if err != nil {
		panic(err)
	}
	fullDirCatalogInnerSyms := fullDirCatalogStuff + "/innerSyms"
	err = os.Mkdir(fullDirCatalogInnerSyms, 0755)
	if err != nil {
		panic(err)
	}
	err = os.Symlink("elho", fullDirCatalogInnerSyms+"/.cvmfscatalog")
	fullDirNameClash, err := os.MkdirTemp(srcDir, "full_dir_name_clash")
	if err != nil {
		panic(err)
	}
	fClash1a, err := os.CreateTemp(fullDirNameClash, "test_file.txt")
	if err != nil {
		panic(err)
	}
	if _, err := fClash1a.Write([]byte{1, 2, 3, 4}); err != nil {
		panic(err)
	}
	if err := fClash1a.Close(); err != nil {
		panic(err)
	}
	fClash1b, err := os.Create(fullDirNameClash + "/." + pathlib.NewPath(fClash1a.Name()).Name() + ".f24d7b797432a7aaf05c29e032faa297277a14f8")
	if err != nil {
		panic(err)
	}
	if _, err := fClash1b.Write([]byte{1, 2, 3, 4}); err != nil {
		panic(err)
	}
	if err := fClash1b.Close(); err != nil {
		panic(err)
	}
	fClash2a, err := os.CreateTemp(fullDirNameClash, "test_file2.txt")
	if err != nil {
		panic(err)
	}
	if _, err := fClash2a.Write([]byte{1, 2, 3, 4}); err != nil {
		panic(err)
	}
	if err := fClash2a.Close(); err != nil {
		panic(err)
	}
	err = os.Mkdir(fullDirNameClash+"/."+pathlib.NewPath(fClash2a.Name()).Name()+".f24d7b797432a7aaf05c29e032faa297277a14f8", 0755)
	if err != nil {
		panic(err)
	}
	fullDirTrailingSpace, err := os.MkdirTemp(srcDir, "full_dir")
	if err != nil {
		panic(err)
	}
	err = os.Mkdir(fullDirTrailingSpace+"/inner_empty_dir", 0755)
	if err != nil {
		panic(err)
	}
	err = os.Mkdir(fullDirTrailingSpace+"/inner_empty_dir ", 0755)
	if err != nil {
		panic(err)
	}
	fTrailingSpace, err := os.Create(fullDirTrailingSpace + "/test_file.txt")
	if err != nil {
		panic(err)
	}
	fTrailingSpace2, err := os.Create(fullDirTrailingSpace + "/test_file.txt ")
	if err != nil {
		panic(err)
	}
	fTrailingSpacePath := pathlib.NewPath(fTrailingSpace.Name())
	fTrailingSpacePath2 := pathlib.NewPath(fTrailingSpace2.Name())
	os.Symlink(fTrailingSpacePath.Name(), fullDir+"/test_sym")
	os.Symlink(fTrailingSpacePath2.Name(), innerDir+"/test_sym ")
	if _, err := fTrailingSpace.Write([]byte{1, 2, 3, 4}); err != nil {
		panic(err)
	}
	if err := fTrailingSpace.Close(); err != nil {
		panic(err)
	}
	if _, err := fTrailingSpace2.Write([]byte{4, 3, 2, 1}); err != nil {
		panic(err)
	}
	if err := fTrailingSpace2.Close(); err != nil {
		panic(err)
	}

	return func(t *testing.T) {
		os.RemoveAll(srcDir)
	}, fullDir, fullDirUpdate, fullDirReplace, fullDirErrReplace, fullDirErrSymReplace, fullDirSymReplace, fullDirDotStuff, fullDirCatalogStuff, fullDirNameClash, fullDirTrailingSpace
}

func setupDeleteTestE2E(t *testing.T) (func(t *testing.T), string, string, string, string, string, string, string, string, string, string) {
	srcDir, err := os.MkdirTemp(pkg.TestingTempDir(), "cvmfs_test_src_dir")
	if err != nil {
		panic(err)
	}
	fullDir, err := os.MkdirTemp(srcDir, "full_dir")
	if err != nil {
		panic(err)
	}
	err = os.Mkdir(fullDir+"/inner_dir", 0755)
	if err != nil {
		panic(err)
	}
	err = os.Mkdir(fullDir+"/inner_empty_dir", 0755)
	if err != nil {
		panic(err)
	}
	f, err := os.Create(fullDir + "/test_file.txt")
	if err != nil {
		panic(err)
	}
	innerf, err := os.Create(fullDir + "/inner_dir/inner_test_file.txt")
	if err != nil {
		panic(err)
	}
	os.Symlink("test_file.txt", fullDir+"/test_file_sym.txt")
	os.Symlink("inner_dir", fullDir+"/inner_dir_sym")
	os.Symlink("inner_test_file.txt", fullDir+"/inner_dir/inner_test_file_sym.txt")
	if _, err := f.Write([]byte{1, 2, 3, 4}); err != nil {
		panic(err)
	}
	if err := f.Close(); err != nil {
		panic(err)
	}
	if _, err := innerf.Write([]byte{4, 3, 2}); err != nil {
		panic(err)
	}
	if err := innerf.Close(); err != nil {
		panic(err)
	}
	// Setup dir to recursively test with
	fullDirEmpty, err := os.MkdirTemp(srcDir, "full_dir_empty")
	if err != nil {
		panic(err)
	}

	fullDirFiles, err := os.MkdirTemp(srcDir, "full_dir_files")
	if err != nil {
		panic(err)
	}
	f1, err := os.Create(fullDirFiles + "/inner_dir")
	if err != nil {
		panic(err)
	}
	if _, err := f1.Write([]byte{1, 2, 3, 4}); err != nil {
		panic(err)
	}
	if err := f1.Close(); err != nil {
		panic(err)
	}
	f2, err := os.Create(fullDirFiles + "/inner_empty_dir")
	if err != nil {
		panic(err)
	}
	if _, err := f2.Write([]byte{1, 2, 3, 4}); err != nil {
		panic(err)
	}
	if err := f2.Close(); err != nil {
		panic(err)
	}
	f3, err := os.Create(fullDirFiles + "/test_file.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f3.Write([]byte{5, 5, 5}); err != nil {
		panic(err)
	}
	if err := f3.Close(); err != nil {
		panic(err)
	}
	f4, err := os.Create(fullDirFiles + "/test_file_sym.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f4.Write([]byte{5, 5, 5}); err != nil {
		panic(err)
	}
	if err := f4.Close(); err != nil {
		panic(err)
	}
	f5, err := os.Create(fullDirFiles + "/inner_dir_sym")
	if err != nil {
		panic(err)
	}
	if _, err := f5.Write([]byte{5, 5, 5}); err != nil {
		panic(err)
	}
	if err := f5.Close(); err != nil {
		panic(err)
	}

	fullDirEmptyDirs, err := os.MkdirTemp(srcDir, "full_dir_empty_dirs")
	if err != nil {
		panic(err)
	}
	err = os.Mkdir(fullDirEmptyDirs+"/inner_dir", 0755)
	if err != nil {
		panic(err)
	}
	err = os.Mkdir(fullDirEmptyDirs+"/inner_empty_dir", 0755)
	if err != nil {
		panic(err)
	}
	err = os.Mkdir(fullDirEmptyDirs+"/test_file.txt", 0755)
	if err != nil {
		panic(err)
	}
	err = os.Mkdir(fullDirEmptyDirs+"/test_file_sym.txt", 0755)
	if err != nil {
		panic(err)
	}
	err = os.Mkdir(fullDirEmptyDirs+"/inner_dir_sym", 0755)
	if err != nil {
		panic(err)
	}

	fullDirFullDirs, err := os.MkdirTemp(srcDir, "full_dir_full_dirs")
	if err != nil {
		panic(err)
	}
	err = os.Mkdir(fullDirFullDirs+"/inner_dir", 0755)
	if err != nil {
		panic(err)
	}
	f6, err := os.Create(fullDirFullDirs + "/inner_dir/t1.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f6.Write([]byte{5, 5, 5}); err != nil {
		panic(err)
	}
	if err := f6.Close(); err != nil {
		panic(err)
	}
	err = os.Mkdir(fullDirFullDirs+"/inner_empty_dir", 0755)
	if err != nil {
		panic(err)
	}
	f7, err := os.Create(fullDirFullDirs + "/inner_empty_dir/t2.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f7.Write([]byte{5, 5, 5}); err != nil {
		panic(err)
	}
	if err := f7.Close(); err != nil {
		panic(err)
	}
	err = os.Mkdir(fullDirFullDirs+"/test_file.txt", 0755)
	if err != nil {
		panic(err)
	}
	f8, err := os.Create(fullDirFullDirs + "/test_file.txt/t3.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f8.Write([]byte{5, 5, 5}); err != nil {
		panic(err)
	}
	if err := f8.Close(); err != nil {
		panic(err)
	}
	err = os.Mkdir(fullDirFullDirs+"/test_file_sym.txt", 0755)
	if err != nil {
		panic(err)
	}
	f9, err := os.Create(fullDirFullDirs + "/test_file_sym.txt/t4.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f9.Write([]byte{5, 5, 5}); err != nil {
		panic(err)
	}
	if err := f9.Close(); err != nil {
		panic(err)
	}
	err = os.Mkdir(fullDirFullDirs+"/inner_dir_sym", 0755)
	if err != nil {
		panic(err)
	}
	f10, err := os.Create(fullDirFullDirs + "/inner_dir_sym/t5.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f10.Write([]byte{5, 5, 5}); err != nil {
		panic(err)
	}
	if err := f10.Close(); err != nil {
		panic(err)
	}

	fullDirSyms, err := os.MkdirTemp(srcDir, "full_dir_syms")
	if err != nil {
		panic(err)
	}
	os.Symlink("firball", fullDirSyms+"/inner_dir")
	os.Symlink("blessing", fullDirSyms+"/inner_empty_dir")
	os.Symlink("vishus_mokry", fullDirSyms+"/test_file.txt")
	os.Symlink("pss_wout_trce", fullDirSyms+"/test_file_sym.txt")
	os.Symlink("cre_wnds", fullDirSyms+"/inner_dir_sym")

	fullDirDotCheck, err := os.MkdirTemp(srcDir, "full_dir_dot_check")
	if err != nil {
		panic(err)
	}
	f11, err := os.Create(fullDirDotCheck + "/test_file1.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f11.Write([]byte{5, 5, 5}); err != nil {
		panic(err)
	}
	if err := f11.Close(); err != nil {
		panic(err)
	}
	f12, err := os.Create(fullDirDotCheck + "/test_file2.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f12.Write([]byte{5, 5, 5}); err != nil {
		panic(err)
	}
	if err := f12.Close(); err != nil {
		panic(err)
	}
	fullDirDotUpdate, err := os.MkdirTemp(srcDir, "full_dir_dot_update")
	if err != nil {
		panic(err)
	}
	f13, err := os.Create(fullDirDotUpdate + "/test_file1.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f13.Write([]byte{5, 5, 5, 4}); err != nil {
		panic(err)
	}
	if err := f13.Close(); err != nil {
		panic(err)
	}
	f14, err := os.Create(fullDirDotUpdate + "/test_file2.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f14.Write([]byte{5, 5, 5, 4}); err != nil {
		panic(err)
	}
	if err := f14.Close(); err != nil {
		panic(err)
	}
	f14Info, err := os.Lstat(f14.Name())
	if err != nil {
		panic(err)
	}
	fullDirDotFinal, err := os.MkdirTemp(srcDir, "full_dir_dot_final")
	if err != nil {
		panic(err)
	}
	f15, err := os.Create(fullDirDotFinal + "/test_file1.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f15.Write([]byte{5, 5, 5, 4, 3}); err != nil {
		panic(err)
	}
	if err := f15.Close(); err != nil {
		panic(err)
	}
	f16, err := os.Create(fullDirDotFinal + "/test_file2.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f16.Write([]byte{5, 5, 5, 4}); err != nil {
		panic(err)
	}
	if err := f16.Close(); err != nil {
		panic(err)
	}
	os.Chtimes(f16.Name(), time.Now().Local(), f14Info.ModTime())
	fullDirOneInner, err := os.MkdirTemp(srcDir, "full_dir_one_inner")
	if err != nil {
		panic(err)
	}
	err = os.Mkdir(fullDirOneInner+"/inner_dir", 0755)
	if err != nil {
		panic(err)
	}

	return func(t *testing.T) {
		os.RemoveAll(srcDir)
	}, fullDir, fullDirEmpty, fullDirFiles, fullDirEmptyDirs, fullDirFullDirs, fullDirSyms, fullDirDotCheck, fullDirDotUpdate, fullDirDotFinal, fullDirOneInner
}

func setupSymDerefTestE2E(t *testing.T) (func(t *testing.T), string, string, string, string, string, string, string, string, string, string, string, string, string, string) {
	srcDir, err := os.MkdirTemp(pkg.TestingTempDir(), "cvmfs_test_src_dir")
	if err != nil {
		panic(err)
	}
	emptyDir, err := os.MkdirTemp(srcDir, "empty_dir")
	if err != nil {
		panic(err)
	}
	emptyDir2, err := os.MkdirTemp(srcDir, "empty_dir2")
	if err != nil {
		panic(err)
	}
	emptyDir3, err := os.MkdirTemp(srcDir, "empty_dir3")
	if err != nil {
		panic(err)
	}
	f, err := os.CreateTemp(srcDir, "test_file.txt")
	if err != nil {
		panic(err)
	}
	symEmptyDir := srcDir + "/empty_dir_sym_deref"
	emptyDirPath := pathlib.NewPath(emptyDir)
	fPath := pathlib.NewPath(f.Name())
	os.Symlink(emptyDirPath.Name(), symEmptyDir)
	symFile := srcDir + "/test_file_sym_deref.txt"
	os.Symlink(fPath.Name(), symFile)
	if _, err := f.Write([]byte{1, 2, 3, 4}); err != nil {
		panic(err)
	}
	if err := f.Close(); err != nil {
		panic(err)
	}

	fullDir2, err := os.MkdirTemp(srcDir, "full_dir2")
	if err != nil {
		panic(err)
	}
	ffd2, err := os.Create(fullDir2 + "/test_file.txt")
	if err != nil {
		panic(err)
	}
	if _, err := ffd2.Write([]byte{1, 2, 3, 4}); err != nil {
		panic(err)
	}
	if err := ffd2.Close(); err != nil {
		panic(err)
	}
	fullDir3, err := os.MkdirTemp(srcDir, "full_dir3")
	if err != nil {
		panic(err)
	}
	ffd3, err := os.Create(fullDir3 + "/test_file.txt")
	if err != nil {
		panic(err)
	}
	if _, err := ffd3.Write([]byte{1, 2, 3, 4}); err != nil {
		panic(err)
	}
	if err := ffd3.Close(); err != nil {
		panic(err)
	}
	fullDir4, err := os.MkdirTemp(srcDir, "full_dir4")
	if err != nil {
		panic(err)
	}
	ffd4, err := os.Create(fullDir4 + "/test_file.txt")
	if err != nil {
		panic(err)
	}
	if _, err := ffd4.Write([]byte{1, 2, 3, 4}); err != nil {
		panic(err)
	}
	if err := ffd4.Close(); err != nil {
		panic(err)
	}

	fullDir, err := os.MkdirTemp(srcDir, "full_dir")
	if err != nil {
		panic(err)
	}
	err = os.Mkdir(fullDir+"/inner_dir", 0755)
	if err != nil {
		panic(err)
	}
	err = os.Mkdir(fullDir+"/inner_empty_dir", 0755)
	if err != nil {
		panic(err)
	}
	f1, err := os.Create(fullDir + "/test_file.txt")
	if err != nil {
		panic(err)
	}
	innerf1, err := os.Create(fullDir + "/inner_dir/inner_test_file.txt")
	if err != nil {
		panic(err)
	}
	os.Symlink("inner_dir", fullDir+"/inner_dir_sym")
	os.Symlink("inner_test_file.txt", fullDir+"/inner_dir/inner_test_file_sym.txt")
	if _, err := f1.Write([]byte{1, 2, 3, 4}); err != nil {
		panic(err)
	}
	if err := f1.Close(); err != nil {
		panic(err)
	}
	if _, err := innerf1.Write([]byte{4, 3, 2, 1}); err != nil {
		panic(err)
	}
	if err := innerf1.Close(); err != nil {
		panic(err)
	}
	symFullDir := srcDir + "/full_dir_sym_deref"
	fullDirPath := pathlib.NewPath(fullDir)
	os.Symlink(fullDirPath.Name(), symFullDir)

	fullDirSymFile, err := os.MkdirTemp(srcDir, "full_dir_symfile")
	if err != nil {
		panic(err)
	}
	f2, err := os.Create(fullDirSymFile + "/test_file_symfile.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f2.Write([]byte{7, 7, 7}); err != nil {
		panic(err)
	}
	if err := f2.Close(); err != nil {
		panic(err)
	}
	os.Symlink("test_file_symfile.txt", fullDirSymFile+"/sym_sym")
	// os.Symlink("test_file_symfile.txt", fullDirSymFile+"/inner_dir")
	os.Symlink("sym_sym", fullDirSymFile+"/inner_empty_dir")
	os.Symlink("sym_sym", fullDirSymFile+"/test_file.txt")
	os.Symlink("sym_sym", fullDirSymFile+"/inner_dir_sym")

	fullDirSymFileDelete, err := os.MkdirTemp(srcDir, "full_dir_symfile_delete")
	if err != nil {
		panic(err)
	}
	f5, err := os.Create(fullDirSymFileDelete + "/test_file_symfile.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f5.Write([]byte{8, 8, 8, 8, 8, 8, 8}); err != nil {
		panic(err)
	}
	if err := f5.Close(); err != nil {
		panic(err)
	}
	os.Symlink("test_file_symfile.txt", fullDirSymFileDelete+"/sym_sym")
	os.Symlink("sym_sym", fullDirSymFileDelete+"/inner_dir")
	os.Symlink("sym_sym", fullDirSymFileDelete+"/inner_empty_dir")
	os.Symlink("sym_sym", fullDirSymFileDelete+"/test_file.txt")
	os.Symlink("sym_sym", fullDirSymFileDelete+"/inner_dir_sym")

	fullDirSymEmptyDir, err := os.MkdirTemp(srcDir, "full_dir_symEmptyDir")
	if err != nil {
		panic(err)
	}
	err = os.Mkdir(fullDirSymEmptyDir+"/inner_sym_empty", 0755)
	if err != nil {
		panic(err)
	}
	os.Symlink("inner_sym_empty", fullDirSymEmptyDir+"/inner_dir")
	os.Symlink("inner_sym_empty", fullDirSymEmptyDir+"/inner_empty_dir")
	os.Symlink("inner_sym_empty", fullDirSymEmptyDir+"/test_file.txt")
	os.Symlink("inner_sym_empty", fullDirSymEmptyDir+"/inner_dir_sym")

	fullDirSymFullDir, err := os.MkdirTemp(srcDir, "full_dir_symFullDir")
	if err != nil {
		panic(err)
	}
	err = os.Mkdir(fullDirSymFullDir+"/inner_sym_full", 0755)
	if err != nil {
		panic(err)
	}
	f3, err := os.Create(fullDirSymFullDir + "/inner_sym_full/t.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f3.Write([]byte{8, 8, 8, 8, 8, 8}); err != nil {
		panic(err)
	}
	if err := f3.Close(); err != nil {
		panic(err)
	}
	os.Symlink("inner_sym_full", fullDirSymFullDir+"/inner_dir")
	os.Symlink("inner_sym_full", fullDirSymFullDir+"/inner_empty_dir")
	os.Symlink("inner_sym_full", fullDirSymFullDir+"/test_file.txt")
	os.Symlink("inner_sym_full", fullDirSymFullDir+"/inner_dir_sym")

	return func(t *testing.T) {
		os.RemoveAll(srcDir)
	}, emptyDir, emptyDir2, emptyDir3, fullDir, fullDir2, fullDir3, fullDir4, symFile, symEmptyDir, symFullDir, fullDirSymFile, fullDirSymFileDelete, fullDirSymEmptyDir, fullDirSymFullDir
}

func setupExcludeTestE2E(t *testing.T) (func(t *testing.T), string, string) {
	srcDir, err := os.MkdirTemp(pkg.TestingTempDir(), "cvmfs_test_src_dir")
	if err != nil {
		panic(err)
	}
	fullDir, err := os.MkdirTemp(srcDir, "full_dir")
	if err != nil {
		panic(err)
	}

	f1, err := os.Create(fullDir + "/exc_test_file.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f1.Write([]byte{8, 8, 8, 8, 8, 8}); err != nil {
		panic(err)
	}
	if err := f1.Close(); err != nil {
		panic(err)
	}
	os.Mkdir(fullDir+"/exc_test_dir", 0755)
	os.Symlink("exc_test_file.txt", fullDir+"/exc_test_file_sym.txt")

	fullDirMulti, err := os.MkdirTemp(srcDir, "full_dir")
	if err != nil {
		panic(err)
	}

	f2, err := os.Create(fullDirMulti + "/exc_test_file.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f2.Write([]byte{9, 9, 9, 9}); err != nil {
		panic(err)
	}
	if err := f2.Close(); err != nil {
		panic(err)
	}
	os.Mkdir(fullDirMulti+"/exc_test_dir", 0755)
	f3, err := os.Create(fullDirMulti + "/exc_test_dir/exc_test_file.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f3.Write([]byte{10, 10, 10, 10, 10, 10, 10}); err != nil {
		panic(err)
	}
	if err := f3.Close(); err != nil {
		panic(err)
	}
	os.Mkdir(fullDirMulti+"/exc_test_dir/exc_test_dir", 0755)
	f4, err := os.Create(fullDirMulti + "/exc_test_dir/exc_test_dir/exc_test_filt.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f4.Write([]byte{10, 10, 11}); err != nil {
		panic(err)
	}
	if err := f4.Close(); err != nil {
		panic(err)
	}
	os.Symlink("test_file.txt", fullDirMulti+"/exc_test_file_sym.txt")

	return func(t *testing.T) {
		os.RemoveAll(srcDir)
	}, fullDir, fullDirMulti
}

func setupExcludeDeleteTestE2E(t *testing.T) (func(t *testing.T), string, string, string) {
	srcDir, err := os.MkdirTemp(pkg.TestingTempDir(), "cvmfs_test_src_dir")
	if err != nil {
		panic(err)
	}
	fullDir, err := os.MkdirTemp(srcDir, "full_dir")
	if err != nil {
		panic(err)
	}

	f1, err := os.Create(fullDir + "/exc_test_file.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f1.Write([]byte{8, 8, 8, 8, 8, 8}); err != nil {
		panic(err)
	}
	if err := f1.Close(); err != nil {
		panic(err)
	}
	os.Mkdir(fullDir+"/exc_test_dir", 0755)
	f5, err := os.Create(fullDir + "/exc_test_dir/exc_test_file.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f5.Write([]byte{8, 8, 8, 7}); err != nil {
		panic(err)
	}
	if err := f5.Close(); err != nil {
		panic(err)
	}
	os.Mkdir(fullDir+"/exc_test_dir_empty", 0755)
	os.Symlink("exc_test_file.txt", fullDir+"/exc_test_file_sym.txt")

	fullDirMulti, err := os.MkdirTemp(srcDir, "full_dir")
	if err != nil {
		panic(err)
	}

	f2, err := os.Create(fullDirMulti + "/exc_test_file.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f2.Write([]byte{9, 9, 9, 9}); err != nil {
		panic(err)
	}
	if err := f2.Close(); err != nil {
		panic(err)
	}
	os.Mkdir(fullDirMulti+"/exc_test_dir", 0755)
	f3, err := os.Create(fullDirMulti + "/exc_test_dir/exc_test_file.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f3.Write([]byte{10, 10, 10, 10, 10, 10, 10}); err != nil {
		panic(err)
	}
	if err := f3.Close(); err != nil {
		panic(err)
	}
	os.Mkdir(fullDirMulti+"/exc_test_dir/exc_test_dir", 0755)
	f4, err := os.Create(fullDirMulti + "/exc_test_dir/exc_test_dir/exc_test_filt.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f4.Write([]byte{10, 10, 11}); err != nil {
		panic(err)
	}
	if err := f4.Close(); err != nil {
		panic(err)
	}
	os.Symlink("test_file.txt", fullDirMulti+"/exc_test_file_sym.txt")

	fullDirMultiDel, err := os.MkdirTemp(srcDir, "full_dir")
	if err != nil {
		panic(err)
	}

	os.Mkdir(fullDirMultiDel+"/exc_test_dir", 0755)
	os.Mkdir(fullDirMultiDel+"/exc_test_dir/exc_test_dir", 0755)
	os.Symlink("test_file.txt", fullDirMultiDel+"/exc_test_file_sym.txt")

	return func(t *testing.T) {
		os.RemoveAll(srcDir)
	}, fullDir, fullDirMulti, fullDirMultiDel
}

func setupPermissionTestE2E(t *testing.T) (func(t *testing.T), string, string, string, string, string, string, string, string, string, string, string) {
	srcDir, err := os.MkdirTemp(pkg.TestingTempDir(), "cvmfs_test_src_dir")
	if err != nil {
		panic(err)
	}
	err = os.Mkdir(srcDir+"/disallow-upload-1", 0755)
	if err != nil {
		panic(err)
	}
	f1, err := os.CreateTemp(srcDir+"/disallow-upload-1", "test_file1.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f1.Write([]byte{4, 3, 2, 1, 0}); err != nil {
		panic(err)
	}
	if err := f1.Close(); err != nil {
		panic(err)
	}
	err = os.Mkdir(srcDir+"/disallow-upload-1/inner_dir", 0755)
	if err != nil {
		panic(err)
	}
	f3, err := os.CreateTemp(srcDir+"/disallow-upload-1/inner_dir", "inner_test_file1.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f3.Write([]byte{5}); err != nil {
		panic(err)
	}
	if err := f3.Close(); err != nil {
		panic(err)
	}
	os.Symlink("test_file1.txt", srcDir+"/disallow-upload-1/test_file1_sym.txt")

	err = os.Mkdir(srcDir+"/disallow-upload-2", 0755)
	if err != nil {
		panic(err)
	}
	err = os.Mkdir(srcDir+"/disallow-upload-2/allow-upload-inner", 0755)
	if err != nil {
		panic(err)
	}

	f2, err := os.CreateTemp(srcDir+"/disallow-upload-2/allow-upload-inner", "test_file2.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f2.Write([]byte{1, 2, 3, 4}); err != nil {
		panic(err)
	}
	if err := f2.Close(); err != nil {
		panic(err)
	}
	err = os.Mkdir(srcDir+"/disallow-upload-2/allow-upload-inner/inner_dir", 0755)
	if err != nil {
		panic(err)
	}
	f4, err := os.CreateTemp(srcDir+"/disallow-upload-2/allow-upload-inner/inner_dir", "inner_test_file2.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f4.Write([]byte{4, 3, 2, 1, 0, 9}); err != nil {
		panic(err)
	}
	if err := f4.Close(); err != nil {
		panic(err)
	}
	os.Symlink("test_file2.txt", srcDir+"/disallow-upload-2/allow-upload-inner/test_file1_sym.txt")

	err = os.Mkdir(srcDir+"/allow-upload-1", 0755)
	if err != nil {
		panic(err)
	}
	f11, err := os.CreateTemp(srcDir+"/allow-upload-1", "test_file1.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f11.Write([]byte{4, 3, 2, 1, 0}); err != nil {
		panic(err)
	}
	if err := f11.Close(); err != nil {
		panic(err)
	}
	err = os.Mkdir(srcDir+"/allow-upload-1/inner_dir", 0755)
	if err != nil {
		panic(err)
	}
	f13, err := os.CreateTemp(srcDir+"/allow-upload-1/inner_dir", "inner_test_file1.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f13.Write([]byte{5}); err != nil {
		panic(err)
	}
	if err := f13.Close(); err != nil {
		panic(err)
	}
	os.Symlink("test_file1.txt", srcDir+"/allow-upload-1/test_file1_sym.txt")

	err = os.Mkdir(srcDir+"/allow-upload-2", 0755)
	if err != nil {
		panic(err)
	}
	err = os.Mkdir(srcDir+"/allow-upload-2/disallow-upload-inner", 0755)
	if err != nil {
		panic(err)
	}

	f12, err := os.CreateTemp(srcDir+"/allow-upload-2/disallow-upload-inner", "test_file2.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f12.Write([]byte{1, 2, 3, 4}); err != nil {
		panic(err)
	}
	if err := f12.Close(); err != nil {
		panic(err)
	}
	err = os.Mkdir(srcDir+"/allow-upload-2/disallow-upload-inner/inner_dir", 0755)
	if err != nil {
		panic(err)
	}
	f14, err := os.CreateTemp(srcDir+"/allow-upload-2/disallow-upload-inner/inner_dir", "inner_test_file2.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f14.Write([]byte{4, 3, 2, 1, 0, 9}); err != nil {
		panic(err)
	}
	if err := f14.Close(); err != nil {
		panic(err)
	}
	os.Symlink("test_file2.txt", srcDir+"/allow-upload-2/disallow-upload-inner/test_file1_sym.txt")

	err = os.Mkdir(srcDir+"/inner", 0755)
	if err != nil {
		panic(err)
	}
	err = os.Mkdir(srcDir+"/disallow-delete-1", 0755)
	if err != nil {
		panic(err)
	}
	err = os.Mkdir(srcDir+"/disallow-delete-2", 0755)
	if err != nil {
		panic(err)
	}
	err = os.Mkdir(srcDir+"/disallow-delete-2/allow-delete-inner", 0755)
	if err != nil {
		panic(err)
	}

	err = os.Mkdir(srcDir+"/allow-delete-1", 0755)
	if err != nil {
		panic(err)
	}

	emptyDir, err := os.MkdirTemp(srcDir, "emptyDir")
	if err != nil {
		panic(err)
	}

	f18, err := os.CreateTemp(srcDir, "different-file.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f18.Write([]byte{4, 3, 9, 8}); err != nil {
		panic(err)
	}
	if err := f18.Close(); err != nil {
		panic(err)
	}
	os.Symlink(pathlib.NewPath(f18.Name()).Name(), srcDir+"/sym_file.txt")

	return func(t *testing.T) {
		os.RemoveAll(srcDir)
	}, srcDir + "/disallow-upload-1", srcDir + "/disallow-upload-2", srcDir + "/allow-upload-1", srcDir + "/allow-upload-2", srcDir + "/disallow-delete-1", srcDir + "/disallow-delete-2", srcDir + "/allow-delete-1", emptyDir, f18.Name(), srcDir + "/sym_file.txt", srcDir + "/inner"
}

func setupPermissionDefaultTestE2E(t *testing.T) (func(t *testing.T), string) {
	srcDir, err := os.MkdirTemp(pkg.TestingTempDir(), "cvmfs_test_src_dir")
	if err != nil {
		panic(err)
	}
	f1, err := os.CreateTemp(srcDir, "test_file_default_checking.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f1.Write([]byte{4, 3, 2, 1, 0, 7, 1}); err != nil {
		panic(err)
	}
	if err := f1.Close(); err != nil {
		panic(err)
	}

	return func(t *testing.T) {
		os.RemoveAll(srcDir)
	}, f1.Name()
}

func addPermissionedDirs(ctx Context) {
	srcDir, err := os.MkdirTemp(pkg.TestingTempDir(), "cvmfs_test_src_dir")
	if err != nil {
		panic(err)
	}
	f1, err := os.CreateTemp(srcDir, "ro-file.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f1.Write([]byte{4, 3}); err != nil {
		panic(err)
	}
	if err := f1.Close(); err != nil {
		panic(err)
	}
	f1Path := pathlib.NewPath(f1.Name())
	hasher := pkg.NewHasher(30, pkg.IOBufferSize)
	f1Stat, err := os.Lstat(f1.Name())
	if err != nil {
		panic(err)
	}
	f1hashData, err := hasher.HashFile(f1Stat, f1Path, pkg.CVMFSChunkSize)
	if err != nil {
		panic(err)
	}
	_, f1PathData, err := pkg.PathExists(f1Path)
	if err != nil {
		panic(err)
	}

	f1HashTag := fmt.Sprintf("%040x", f1hashData.Checksum)

	// Should probably create an initializer here for the database struct now that it exists
	database, err := pkg.NewCvmfsGraftingDB()
	if err != nil {
		panic(err)
	}

	uid := os.Geteuid()
	gid := os.Getegid()
	groupObj, err := user.LookupGroupId(strconv.Itoa(gid))
	if err != nil {
		panic(err)
	}
	groupname := groupObj.Name

	aclDirs := []PathNameMode{
		PathNameMode{PathString: "check-permissions4", Mode: 365},
	}
	dirsToCreate := []PathNameMode{
		PathNameMode{PathString: "disallow-upload", Mode: 493},
		PathNameMode{PathString: "disallow-upload/allow-upload-inner", Mode: 493},
		PathNameMode{PathString: "allow-upload", Mode: 493},
		PathNameMode{PathString: "allow-upload/disallow-upload-inner", Mode: 493},
		PathNameMode{PathString: "disallow-delete", Mode: 493},
		PathNameMode{PathString: "disallow-delete/allow-delete-inner", Mode: 493},
		PathNameMode{PathString: "disallow-delete/inner", Mode: 493},
		PathNameMode{PathString: "allow-delete", Mode: 493},
		PathNameMode{PathString: "allow-delete/disallow-delete-inner", Mode: 493},
		PathNameMode{PathString: "check-permissions", Mode: 365},
		PathNameMode{PathString: "check-permissions2", Mode: 511},
		PathNameMode{PathString: "check-permissions2/inner-dir", Mode: 365},
		PathNameMode{PathString: "check-permissions3", Mode: 365},
		PathNameMode{PathString: "check-permissions4/inner-dir", Mode: 365},
		PathNameMode{PathString: "no-check-permissions", Mode: 511},
		PathNameMode{PathString: "no-check-permissions/inner-dir", Mode: 365},
		PathNameMode{PathString: "no-check-permissions/inner-dir2", Mode: 365},
		PathNameMode{PathString: "no-check-permissions/inner-dir2/garb", Mode: 365},
	}
	filesToCreate := []PathNameMode{
		PathNameMode{PathString: "disallow-delete/inner/inner_file.txt", Mode: 511},
		PathNameMode{PathString: "check-permissions/rwx-file", Mode: 511},

		PathNameMode{PathString: "check-permissions2/ro-file", Mode: 292},
		PathNameMode{PathString: "check-permissions2/writeable-file", Mode: 511},
		PathNameMode{PathString: "no-check-permissions/ro-file", Mode: 292},
		PathNameMode{PathString: "no-check-permissions/writeable-file", Mode: 511},

		PathNameMode{PathString: "check-permissions3/rwx-file", Mode: 511},
		PathNameMode{PathString: "check-permissions4/ro-file", Mode: 292},
		PathNameMode{PathString: "check-permissions4/writeable-file", Mode: 511},
	}

	for _, pathNM := range aclDirs {
		if err := database.InsertDir(pathNM.PathString, pathNM.Mode, time.Now().UnixNano(), uid, gid, fmt.Sprintf("user::rwx,group::r-x,group:%s:rwx,mask::rwx,other::r-x", groupname)); err != nil {
			panic(err)
		}
	}
	for _, pathNM := range dirsToCreate {
		if err := database.InsertDir(pathNM.PathString, pathNM.Mode, time.Now().UnixNano(), uid, gid, "user::rwx,group::r-x,other::r-x"); err != nil {
			panic(err)
		}
	}

	if ctx.cfg.Repo.DotScheme {
		for _, pathNM := range filesToCreate {
			filePath := pathlib.NewPath(pathNM.PathString)
			if err := database.InsertLink(pathNM.PathString, "."+filePath.Name()+"."+f1HashTag, time.Now().UnixNano(), uid, gid, pkg.EXTERNAL); err != nil {
				panic(err)
			}
			filePathParts := filePath.Parts()
			dotFilePathString := strings.Join(append(filePathParts[0:len(filePathParts)-1], "."+filePath.Name()+"."+f1HashTag), pkg.FileDelimeter)
			if err := database.InsertFile(dotFilePathString, f1.Name(), pathNM.Mode, time.Now().UnixNano(), uid, gid, f1PathData.Size(), strings.Join(pkg.HashesToStrings(f1hashData.Hashes), ","), fmt.Sprintf("%040x", f1hashData.Checksum), f1PathData, pkg.EXTERNAL, false); err != nil {
				panic(err)
			}
		}
	} else {
		for _, pathNM := range filesToCreate {
			if err := database.InsertFile(pathNM.PathString, f1.Name(), pathNM.Mode, time.Now().UnixNano(), uid, gid, f1PathData.Size(), strings.Join(pkg.HashesToStrings(f1hashData.Hashes), ","), fmt.Sprintf("%040x", f1hashData.Checksum), f1PathData, pkg.EXTERNAL, false); err != nil {
				panic(err)
			}
		}
	}

	pkg.Mock_graft_getter()(database, "", "", true)
	pkg.UmountRepo()
	time.Sleep(500 * time.Millisecond)
	pkg.MountRepo()
	if err := database.Teardown(true); err != nil {
		panic(err)
	}
}

func setupRelativeTestE2E(t *testing.T) (func(t *testing.T), string, string, string, string, string, string, string, string) {
	srcDir, err := os.MkdirTemp(pkg.TestingTempDir(), "cvmfs_test_src_dir")
	if err != nil {
		panic(err)
	}
	fullDir, err := os.MkdirTemp(srcDir, "a")
	if err != nil {
		panic(err)
	}
	innerDir := fullDir + "/b"
	err = os.Mkdir(innerDir, 0755)
	if err != nil {
		panic(err)
	}
	innerEmptyDir := fullDir + "/c"
	err = os.Mkdir(innerEmptyDir, 0755)
	if err != nil {
		panic(err)
	}
	f, err := os.CreateTemp(fullDir, "test_file.txt")
	if err != nil {
		panic(err)
	}
	innerf, err := os.CreateTemp(innerDir, "inner_test_file.txt")
	if err != nil {
		panic(err)
	}
	innerDirPath := pathlib.NewPath(innerDir)
	innerfPath := pathlib.NewPath(innerf.Name())
	os.Symlink(innerDirPath.Name(), fullDir+"/inner_dir_sym")
	os.Symlink(innerfPath.Name(), innerDir+"/inner_test_file_sym.txt")
	if _, err := f.Write([]byte{1, 2, 3, 4}); err != nil {
		panic(err)
	}
	if err := f.Close(); err != nil {
		panic(err)
	}
	if _, err := innerf.Write([]byte{4, 3, 2, 1}); err != nil {
		panic(err)
	}
	if err := innerf.Close(); err != nil {
		panic(err)
	}
	// Setup dir to recursively test with
	fullDir2, err := os.MkdirTemp(srcDir, "x")
	if err != nil {
		panic(err)
	}
	innerDir2 := fullDir2 + "/b"
	err = os.Mkdir(innerDir2, 0755)
	if err != nil {
		panic(err)
	}
	innerEmptyDir2 := fullDir2 + "/c"
	err = os.Mkdir(innerEmptyDir2, 0755)
	if err != nil {
		panic(err)
	}
	innerf2, err := os.CreateTemp(innerDir2, "inner_test_file.txt")
	if err != nil {
		panic(err)
	}
	innerfPath2 := pathlib.NewPath(innerf2.Name())
	os.Symlink(innerfPath2.Name(), innerDir2+"/inner_test_file_sym.txt")
	if _, err := innerf2.Write([]byte{4, 3, 2, 1, 0}); err != nil {
		panic(err)
	}
	if err := innerf2.Close(); err != nil {
		panic(err)
	}

	return func(t *testing.T) {
		os.RemoveAll(srcDir)
	}, fullDir, innerDir, innerEmptyDir, innerf.Name(), fullDir2, innerDir2, innerEmptyDir2, innerf2.Name()
}

func setupDirsFlagTestE2E(t *testing.T) (func(t *testing.T), string, string, string, string, string, string) {
	srcDir, err := os.MkdirTemp(pkg.TestingTempDir(), "cvmfs_test_src_dir")
	if err != nil {
		panic(err)
	}
	fullDir, err := os.MkdirTemp(srcDir, "full_dir")
	if err != nil {
		panic(err)
	}
	fullDirPath := pathlib.NewPath(fullDir)
	innerDir, err := os.MkdirTemp(fullDir, "inner_dir")
	if err != nil {
		panic(err)
	}
	_, err = os.MkdirTemp(fullDir, "inner_empty_dir")
	if err != nil {
		panic(err)
	}
	f, err := os.CreateTemp(fullDir, "test_file.txt")
	if err != nil {
		panic(err)
	}
	innerf, err := os.CreateTemp(innerDir, "inner_test_file.txt")
	if err != nil {
		panic(err)
	}
	innerDirPath := pathlib.NewPath(innerDir)
	innerfPath := pathlib.NewPath(innerf.Name())
	os.Symlink(innerDirPath.Name(), fullDir+"/inner_dir_sym")
	os.Symlink(innerfPath.Name(), innerDir+"/inner_test_file_sym.txt")
	if _, err := f.Write([]byte{1, 2, 3, 4}); err != nil {
		panic(err)
	}
	if err := f.Close(); err != nil {
		panic(err)
	}
	if _, err := innerf.Write([]byte{4, 3, 2, 1}); err != nil {
		panic(err)
	}
	if err := innerf.Close(); err != nil {
		panic(err)
	}
	err = os.Mkdir(srcDir+"/hold_dir", 0755)
	if err != nil {
		panic(err)
	}
	err = os.Mkdir(srcDir+"/hold_dir/"+fullDirPath.Name(), 0755)
	if err != nil {
		panic(err)
	}

	return func(t *testing.T) {
		os.RemoveAll(srcDir)
	}, fullDir, f.Name(), innerDir + "/inner_test_file_sym.txt", innerDir, innerf.Name(), srcDir + "/hold_dir"
}
