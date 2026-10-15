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

var CVMFS_RSYNC_VERSION = "unstable"
var OVERRIDE_CONFIG = "false"
var OVERRIDE_CONFIG_FLAG_SET = false
var OVERRIDE_CONFIG_PATH = ""

type Context struct {
	cfg           pkg.ConfStruct
	recursive     bool
	purge         bool
	debug         bool
	uid           int
	uidString     string
	groupIdMap    map[int]bool
	hostname      string
	bu            string
	numCpus       int
	coreAllotment int
	numWorkers    int
	dryrun        bool
	priority      string
}

func rm(ctx Context, deletePaths []*pathlib.Path, repo *pathlib.Path) (err error) {
	revisionNum := pkg.MissingMetric
	revisionNumBytes, err := xattr.Get(repo.Clean().String(), pkg.MountRevisionXattr)
	if err != nil {
		log.Error().Err(err).Msg("Error getting revision")
	} else {
		revisionNum = string(revisionNumBytes)
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

	if err = processDeletes(ctx, deletePaths, repo, db); err != nil {
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

		if ctx.purge && len(db.QueryPurges()) > 0 {
			if err = purge(ctx, db); err != nil {
				return err
			}
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
var getRepoPath = pkg.GetRepoPath
var getCvmfsConfInfo = pkg.GetCvmfsConfigurationInfo
var destInCvmfs = pkg.DestInCvmfs

func launchRm(ctx Context, deletePaths []*pathlib.Path) error {
	var err error
	// Get the destPath from its string (real and ghost)
	relPaths := []*pathlib.Path{}
	firstDir := true
	var repo *pathlib.Path
	for _, deletePath := range deletePaths {
		if deletePath.Name() == pkg.PreviousDirectory || deletePath.Name() == pkg.CurrentDirectory || deletePath.Name() == pkg.CVMFSProtectedFile || deletePath.Name() == pkg.CVMFSAutoProtectedFile {
			log.Warn().Str("Skipping Path", deletePath.String()).Msg("Refusing to remove `.`, `..`, or cvmfs catalog")
			continue
		}
		deletePath = deletePath.Clean()
		absParentPath, err := pkg.GetAbsolutePath(deletePath.Parent())
		if err != nil {
			return err
		}
		absPath := absParentPath.Join(deletePath.Name())

		log.Debug().Str("Absolute Path", absPath.String()).Str("Absolute Parent Path", absParentPath.String()).Msg("Path being deleted")

		// Ensure only src or dest is in cvmfs
		destIn, err := destInCvmfs(absParentPath)
		if err != nil {
			return err
		}
		if !destIn {
			err := fmt.Errorf("every path should be in cvmfs")
			log.Error().Err(err).Msg("At least one path not in cvmfs")
			return err
		}

		// Get the repo being copied too
		var deleteRelative *pathlib.Path
		if firstDir {
			repo, deleteRelative, err = getRepoPath(absPath)
			if err != nil {
				return err
			}
		} else {
			deleteRelative, err = absPath.RelativeTo(repo)
			if err != nil {
				err := fmt.Errorf("directory path is not relative to repository")
				log.Error().Err(err).Msg("You mau be trying to upload to separate or non repositories. Please make sure every path is in the same cvmfs repo.")
			}
		}
		if deleteRelative.Clean().String() == pkg.CurrentDirectory {
			err := fmt.Errorf("cannot delete root")
			log.Error().Err(err).Msg("Please do not delete the root of the cvmfs repo.")
			return err
		}
		relPaths = append(relPaths, deleteRelative)
	}

	if repo != nil {
		ctx.cfg, ctx.uidString, ctx.uid, ctx.groupIdMap, err = getCvmfsConfInfo(repo.Name(), pkg.GetConfigFileForRepo(repo.Name(), OVERRIDE_CONFIG_FLAG_SET, OVERRIDE_CONFIG_PATH))
		if err != nil {
			log.Error().Err(err).Msg("Error parsing config")
			return err
		}

		if err = rm(ctx, relPaths, repo); err != nil {
			log.Error().Err(err).Msg("Rm processing had a fatal error")
			return err
		}
	} else {
		log.Info().Msg("Nothing to rm, skipping rm.")
	}
	return nil
}

func getAutotunedCtx(ctx Context) Context {
	logLine := log.Debug() // Autotuning is now hidden without debug. We can deduce easily from number of cores.
	newCtx := ctx
	numAllotedCpus := ctx.numCpus
	if ctx.coreAllotment > 0 {
		numAllotedCpus = ctx.coreAllotment
	}

	newCtx.numWorkers = min(numAllotedCpus*WorkerScalar, WorkerMax)
	logLine.Int("num purge workers", newCtx.numWorkers).Msg("rm using these parameters")
	log.Info().Int("Allotted Cores", numAllotedCpus).Msg("Cvmfs rming with these parameters (Only affects purging)")
	return newCtx
}

func getInfoFromFlags() (Context, []*pathlib.Path, error) {
	flagSet := flag.NewFlagSet("flags", flag.ExitOnError)

	flagSet.Usage = func() {
		fmt.Fprintf(os.Stderr, "\nThis is the cvmfs_rm tool, meant to implement rm functionality for the CVMFS file "+
			" system.\n\nUsage: cvmfs_rm [OPTION]... FILE...\nTool must be called in a CVMFS directory.\n\n"+
			"Memory Estimation:\n"+
			"Generally this will be <1GB.\n"+
			"\n\n\n\nOptions\n")
		flagSet.PrintDefaults()
	}

	r := flagSet.BoolP("recursive", "r", false, "Remove dirs and contents recursively.")
	purge := flagSet.Bool("purge", false, "Purge directories.")
	flagSet.Int("num-workers", 8, "Sets the number of purge worker threads")
	coreAllotment := flagSet.Int("core-allotment", -1, "Automatically tunes your rsync run as if it had this number of cores alloted. Setting this value <1 instead (recommended) uses the number of cpus available as this value (maximum of 8, unless running in a slurm job which reads from slurm variable). Note, this is only applicable for purging.")
	debug := flagSet.Bool("debug", false, "Add debug logging.")
	dryrun := flagSet.BoolP("dry-run", "n", false, "Report on changes that would be made without uploading objects or making changes to CVMFS.")
	file := flagSet.StringP("file", "f", "", "Remove the paths from a line separated list of paths relative to a provided root directory (a la cvmfs_insert). Ex. cvmfs_rm <repo_root> --file <path_file>")
	flagSet.MarkHidden("num-workers")
	priority := flagSet.StringP("priority", "P", pkg.LowPriority, "Priority of cvmfs_rsync job (low, med, high) (Note: In rare cases, low and med jobs can be serviced before high priority jobs). THIS REQUIRES AN UPGRADED GATEWAY TO USE. Please reach out to linux to get this set up, or if you are unsure if your gateway is upgraded.")

	err := flagSet.Parse(os.Args[1:])
	if err != nil {
		log.Error().Err(err).Msg("Flag parsing error")
		return Context{}, nil, err
	}

	if flagSet.NArg() < 1 {
		err := fmt.Errorf("at least 1 argument needed")
		log.Error().Err(err).Msg("This tool needs 1 or more arguments, use --help for more info.")
		return Context{}, nil, err
	}

	ctx := Context{
		recursive:     *r,
		purge:         *purge,
		debug:         *debug,
		coreAllotment: *coreAllotment,
		dryrun:        *dryrun,
		priority:      *priority,
	}

	vals := flagSet.Args()
	filesToChange := []*pathlib.Path{}
	if flagSet.Changed("file") {
		if flagSet.NArg() > 1 {
			err := fmt.Errorf("only 1 argument allowed")
			log.Error().Err(err).Msg("When using file mode, only one argument is allowed. Use --help for more info.")
			return Context{}, nil, err
		}
		filesToChange, err = pkg.GetPathsFromFile(vals[0], *file)
		if err != nil {
			return Context{}, nil, err
		}
	} else {
		for i := 0; i < len(vals); i++ {
			filesToChange = append(filesToChange, pathlib.NewPath(vals[i]))
		}
	}
	return ctx, filesToChange, nil
}

func Execute() error {
	if OVERRIDE_CONFIG == pkg.TrueString {
		OVERRIDE_CONFIG_FLAG_SET = true
	}
	pkg.SetupLogger()
	ctx, dirPaths, err := getInfoFromFlags()
	if err != nil {
		log.Error().Err(err).Msg("Error parsing flags")
		return err
	}
	if ctx.debug {
		pkg.SetupDebugLogger()
	}

	ctx.numCpus = pkg.SetMaxProcs()
	ctx = getAutotunedCtx(ctx)

	log.Info().Msg("Starting Rm")
	log.Debug().Msg("Debug Mode")
	// CHANGE OVERRIDE TO FALSE BEFORE DEPLOY
	if err = launchRm(ctx, dirPaths); err != nil {
		log.Error().Err(err).Msg("Launch rsync failed")
		return err
	}
	return nil
}
