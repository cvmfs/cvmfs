package main

import (
	"fmt"
	"os"

	pathlib "github.com/chigopher/pathlib"
	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/pkg"
	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/xattr"
	"github.com/rs/zerolog/log"
	flag "github.com/spf13/pflag"
)

var OVERRIDE_CONFIG = "false"
var OVERRIDE_CONFIG_FLAG_SET = false
var OVERRIDE_CONFIG_PATH = ""

type Context struct {
	cfg        pkg.ConfStruct
	symbolic   bool
	force      bool
	debug      bool
	uid        int
	groupIdMap map[int]bool
	noDest     bool
	dryrun     bool
	noDeref    bool
	priority   string
}

func ln(ctx Context, srcLinks []*pathlib.Path, destRelative, destGhostPath, repo *pathlib.Path) (err error) {
	revisionNum := pkg.MissingMetric
	revisionNumBytes, err := xattr.Get(repo.Clean().String(), pkg.MountRevisionXattr)
	if err != nil {
		log.Error().Err(err).Msg("Error getting revision")
	} else {
		revisionNum = string(revisionNumBytes)
	}
	if !ctx.symbolic {
		err = fmt.Errorf("Unimplemented")
		log.Error().Err(err).Msg("Hard links are not yet implemented, please specify -s to create a symlink")
		return err
	}

	destIsDir := true
	destIsSym := true
	if destGhostPath == nil {
		destIsDir, err = repo.JoinPath(destRelative).IsDir()
		if os.IsNotExist(err) {
			destIsDir = false
			err = nil
		} else if err != nil {
			log.Error().Err(err).Msg("Error determining if dest is dir")
			return err
		} else {
			destIsSym, err = repo.JoinPath(destRelative).IsSymlink()
			if err != nil {
				log.Error().Err(err).Msg("Error determining if dest is sym")
				return err
			}
			destIsDir = destIsDir && !destIsSym
		}
	}

	ctx.cfg = pkg.GetBasePathPrefix(ctx.cfg, destRelative)

	canWrite, err := pkg.UserCanWriteDir(ctx.cfg, destRelative, repo, destIsDir, ctx.groupIdMap, ctx.uid)
	if err != nil {
		return err
	}
	if !canWrite {
		err = fmt.Errorf("permission error")
		log.Error().Err(err).Str("Path", destRelative.Clean().String()).Msg("You do not have write permissions to path, quitting")
		return err
	}

	db, err := pkg.NewCvmfsGraftingDB()
	if err != nil {
		log.Error().Err(err).Msg("Error getting grafting db")
		return err
	}
	defer func() {
		if tempErr := db.Teardown(err == nil); tempErr != nil {
			log.Error().Err(tempErr).Msg("Error in cleanup")
			if err == nil {
				err = tempErr
			}
		}
	}()

	if destGhostPath != nil {
		ctx.cfg = pkg.GetBasePathPrefix(ctx.cfg, destRelative.JoinPath(destGhostPath))
		destIsDir = len(srcLinks) > 1
		if destIsDir {
			if err = pkg.CreateGhostPathCVMFS(ctx.cfg, destRelative, destGhostPath, db); err != nil {
				return err
			}
		} else {
			if err = pkg.CreateGhostPathCVMFS(ctx.cfg, destRelative, destGhostPath.Parent(), db); err != nil {
				return err
			}
		}
		destRelative = destRelative.JoinPath(destGhostPath)
	}

	if err = processSymlinks(ctx, srcLinks, destRelative, repo, destIsDir, db); err != nil {
		return err
	}

	if !ctx.dryrun {
		dbEmpty, err := db.IsDatabaseEmpty()
		if err != nil {
			return err
		}
		if !dbEmpty {
			graftMetrics, err := graft(db, repo.Name(), ctx.priority, ctx.debug)
			if err != nil {
				return err
			}
			revisionNum = graftMetrics.Revision
		} else {
			log.Info().Msg("Nothing to graft, skipping grafting step.")
		}
	}

	// Prints dryrun if specified (logs changelog regardless)
	if err = pkg.CreateChangelog(ctx.dryrun, nil, db, revisionNum); err != nil {
		return err
	}

	return err
}

// Testing convenience function
var graft = pkg.Graft
var destInCvmfs = pkg.DestInCvmfs
var getRepoPath = pkg.GetRepoPath

func launchLn(ctx Context, srcPathStrings []string, destPathString string) error {
	var err error
	// Process multiple srcs
	srcPaths := []*pathlib.Path{}
	for _, srcPathString := range srcPathStrings {
		srcPaths = append(srcPaths, pathlib.NewPath(srcPathString))
	}
	// Get the destPath from its string (real and ghost)
	var destPath, destRealPath, destGhostPath *pathlib.Path
	if ctx.noDest {
		if len(srcPathStrings) > 1 {
			err := fmt.Errorf("malformed request")
			log.Error().Err(err).Msg("When not specifying a destination, you may only have one target")
		}
		destRealPath, err = pkg.GetAbsolutePath(pathlib.NewPath(pkg.CurrentDirectory))
		if err != nil {
			return err
		}
		destGhostPath = nil
	} else {
		destPath = pathlib.NewPath(destPathString)
		if destPath.Name() == pkg.CVMFSProtectedFile || destPath.Name() == pkg.CVMFSAutoProtectedFile {
			err = fmt.Errorf("protected file mod")
			log.Warn().Str("Protected File", destPath.String()).Msg("Refusing to modify cvmfs catalog")
			return err
		}
		destPath = destPath.Clean()
		if ctx.noDeref {
			destPathName := destPath.Name()
			destParentRealPath, destParentGhostPath, err := pkg.LongestRealPath(destPath.Parent())
			if err != nil {
				return err
			}
			if destParentGhostPath == nil {
				destPathNoDeref := destParentRealPath.Join(destPathName)
				if _, err := os.Lstat(destPathNoDeref.Clean().String()); os.IsNotExist(err) { // necessary
					destRealPath = destParentRealPath
					destGhostPath = pathlib.NewPath(destPathName)
				} else if err != nil {
					log.Error().Err(err).Str("Str", destPathNoDeref.Clean().String()).Msg("Path failed on seeing if it exists in longest real path")
					return err
				} else {
					destRealPath = destParentRealPath.Join(destPathName)
					destGhostPath = nil
				}
			} else {
				destRealPath = destParentRealPath
				destGhostPath = destParentGhostPath.Join(destPathName)
			}
		} else {
			destRealPath, destGhostPath, err = pkg.LongestRealPath(destPath)
			if err != nil {
				return err
			}
		}
	}
	// Ensure only src or dest is in cvmfs
	destIn, err := destInCvmfs(destRealPath)
	if err != nil {
		return err
	}
	if !destIn {
		err := fmt.Errorf("at least one path not in cvmfs")
		log.Error().Err(err).Msg("Every path should be in cvmfs")
		return err
	}
	// Get the repo being copied too
	repo, destRelative, err := getRepoPath(destRealPath)
	if err != nil {
		return err
	}

	ctx.cfg, _, ctx.uid, ctx.groupIdMap, err = pkg.GetCvmfsConfigurationInfo(repo.Name(), pkg.GetConfigFileForRepo(repo.Name(), OVERRIDE_CONFIG_FLAG_SET, OVERRIDE_CONFIG_PATH))
	if err != nil {
		log.Error().Err(err).Msg("Error parsing config")
		return err
	}

	if err = ln(ctx, srcPaths, destRelative, destGhostPath, repo); err != nil {
		log.Error().Err(err).Msg("ln processing had a fatal error")
		return err
	}
	return nil
}

// Probably will keep this single threaded for now
// func setMaxProcs() error {
// 	maxProcs, slurmJobExists := os.LookupEnv(SlurmJobCpusPerNode)
// 	if slurmJobExists {
// 		slurmJobCpus, err := strconv.Atoi(maxProcs)
// 		if err != nil {
// 			log.Error().Err(err).Str("maxProcs", maxProcs).Msg("Failed string conversion to int for str")
// 			return err
// 		}
// 		runtime.GOMAXPROCS(slurmJobCpus)
// 	} else {
// 		runtime.GOMAXPROCS(runtime.NumCPU())
// 	}
// 	return nil
// }

func getInfoFromFlags() (Context, []string, string, error) {
	flagSet := flag.NewFlagSet("flags", flag.ExitOnError)

	flagSet.Usage = func() {
		fmt.Fprintf(os.Stderr, "\nThis is the cvmfs_ln tool, meant to implement ln functionality for the CVMFS file "+
			" system.\n\nUsage: cvmfs_ln [OPTION]... TARGET LINK_NAME\ncvmfs_ln [OPTION]... TARGET\n"+
			"cvmfs_ln [OPTION]... TARGET... DIRECTORY\nTARGET and DEST must be in CVMFS.\n\nCurrently only designed for"+
			" symlinks.\n\n"+
			"Memory Estimation:\n"+
			"Generally this will be <1GB.\n"+
			"\n\n\n\nOptions\n")
		flagSet.PrintDefaults()
	}

	s := flagSet.BoolP("symbolic", "s", false, "Create a symbolic link instead of hard link.")
	f := flagSet.BoolP("force", "f", false, "Delete existing files.")
	debug := flagSet.Bool("debug", false, "Add debug logging.")
	dryrun := flagSet.BoolP("dry-run", "n", false, "Report on changes that would be made without uploading objects or making changes to CVMFS.")
	noDeref := flagSet.BoolP("no-dereference", "N", false, "Do not dereference the final path component in ln processing (allows for symlinks pointing to dirs to be changed)")
	priority := flagSet.StringP("priority", "P", pkg.LowPriority, "Priority of cvmfs_rsync job (low, med, high) (Note: In rare cases, low and med jobs can be serviced before high priority jobs). THIS REQUIRES AN UPGRADED GATEWAY TO USE. Please reach out to linux to get this set up, or if you are unsure if your gateway is upgraded.")

	err := flagSet.Parse(os.Args[1:])
	if err != nil {
		log.Error().Err(err).Msg("Flag parsing error")
		return Context{}, nil, "", err
	}

	if flagSet.NArg() < 1 {
		err := fmt.Errorf("at least 1 argument needed")
		log.Error().Err(err).Msg("This tool needs 1 or more arguments, use --help for more info.")
		return Context{}, nil, "", err
	}

	ctx := Context{
		symbolic: *s,
		force:    *f,
		debug:    *debug,
		dryrun:   *dryrun,
		noDeref:  *noDeref,
		priority: *priority,
	}

	vals := flagSet.Args()
	var targetPathStrings []string
	var destPathString string
	if len(vals) == 1 {
		targetPathStrings = []string{vals[0]}
		destPathString = EmptyString
		ctx.noDest = true
	} else {
		for i := 0; i < len(vals)-1; i++ {
			targetPathStrings = append(targetPathStrings, vals[i])
		}
		destPathString = vals[len(vals)-1]
		ctx.noDest = false
	}
	return ctx, targetPathStrings, destPathString, nil
}

func main() {
	if OVERRIDE_CONFIG == pkg.TrueString {
		OVERRIDE_CONFIG_FLAG_SET = true
	}
	pkg.SetupLogger()
	ctx, srcPathStrings, destPathString, err := getInfoFromFlags()
	if err != nil {
		log.Error().Err(err).Msg("Error parsing flags")
	}
	if ctx.debug {
		pkg.SetupDebugLogger()
	}

	log.Info().Msg("Starting Ln")
	log.Debug().Msg("Debug Mode")
	// CHANGE OVERRIDE TO FALSE BEFORE DEPLOY
	if err = launchLn(ctx, srcPathStrings, destPathString); err != nil {
		log.Error().Err(err).Msg("Launch ln failed")
		os.Exit(1)
	}
}
