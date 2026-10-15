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
	modeSet    bool
	mode       string
	parent     bool
	debug      bool
	uid        int
	groupIdMap map[int]bool
	dryrun     bool
	priority   string
	faclFile   string
}

type RelGhost struct {
	relative *pathlib.Path
	ghost    *pathlib.Path
}

func mkdir(ctx Context, dirRelGhostPaths []RelGhost, repo *pathlib.Path) (err error) {
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

	var aclString = ""
	if ctx.faclFile != "" {
		aclString, err = pkg.GetAclFromFile(ctx.faclFile)
		if err != nil {
			return err
		}
	}

	if err = processDirs(ctx, dirRelGhostPaths, repo, aclString, db); err != nil {
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
var getRepoPath = pkg.GetRepoPath
var getCvmfsConfInfo = pkg.GetCvmfsConfigurationInfo
var destInCvmfs = pkg.DestInCvmfs

func launchMkdir(ctx Context, dirPaths []*pathlib.Path) error {
	var err error
	// Get the destPath from its string (real and ghost)
	dirRelGhost := []RelGhost{}
	firstDir := true
	var repo *pathlib.Path
	for _, dirPath := range dirPaths {
		dirPath = dirPath.Clean()
		dirRealPath, dirGhostPath, err := pkg.LongestRealPath(dirPath)
		if err != nil {
			return err
		}
		if dirGhostPath == nil && !ctx.parent {
			err = fmt.Errorf("file already exists")
			log.Error().Err(err).Msg("Cannot create dir, path already exists.")
			return err
		}
		// Ensure only src or dest is in cvmfs
		destIn, err := destInCvmfs(dirRealPath)
		if err != nil {
			return err
		}
		if !destIn {
			err := fmt.Errorf("every path should be in cvmfs")
			log.Error().Err(err).Msg("At least one path not in cvmfs")
			return err
		}

		// Get the repo being copied too
		var dirRelative *pathlib.Path
		if firstDir {
			repo, dirRelative, err = getRepoPath(dirRealPath)
			if err != nil {
				return err
			}
		} else {
			dirRelative, err = dirRealPath.RelativeTo(repo)
			if err != nil {
				err := fmt.Errorf("directory path is not relative to repository")
				log.Error().Err(err).Msg("You may be trying to upload to separate or non repositories. Please make sure every path is in the same cvmfs repo.")
			}
		}
		dirRelGhost = append(dirRelGhost, RelGhost{relative: dirRelative, ghost: dirGhostPath})
	}

	ctx.cfg, _, ctx.uid, ctx.groupIdMap, err = getCvmfsConfInfo(repo.Name(), pkg.GetConfigFileForRepo(repo.Name(), OVERRIDE_CONFIG_FLAG_SET, OVERRIDE_CONFIG_PATH))
	if err != nil {
		log.Error().Err(err).Msg("Error parsing config")
		return err
	}

	if err = mkdir(ctx, dirRelGhost, repo); err != nil {
		log.Error().Err(err).Msg("Mkdir processing had a fatal error")
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

func getInfoFromFlags() (Context, []*pathlib.Path, error) {
	flagSet := flag.NewFlagSet("flags", flag.ExitOnError)

	flagSet.Usage = func() {
		fmt.Fprintf(os.Stderr, "\nThis is the cvmfs_mkdir tool, meant to implement mkdir functionality for the CVMFS file "+
			" system.\n\nUsage: cvmfs_mkdir [OPTION]... DIRECTORY...\nTool must be called in a CVMFS directory.\n\n"+
			"Memory Estimation:\n"+
			"Generally this will be <1GB.\n"+
			"\n\n\n\nOptions\n")
		flagSet.PrintDefaults()
	}

	m := flagSet.StringP("mode", "m", "", "Create directory with given mode arguments.")
	p := flagSet.BoolP("parent", "p", false, "Create parent directories.")
	debug := flagSet.Bool("debug", false, "Add debug logging.")
	dryrun := flagSet.BoolP("dry-run", "n", false, "Report on changes that would be made without uploading objects or making changes to CVMFS.")
	file := flagSet.StringP("file", "f", "", "Create the directories from a line separated list of directories relative to a provided root directory (a la cvmfs_insert). Ex. cvmfs_mkdir <repo_root> --file <dir_file>")
	aclFile := flagSet.String("acl-file", "", "Takes in path to acl file. Create directory(s) with given acl file as acl(s) (note, must have allow-acl enabled).")
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
		parent:   *p,
		debug:    *debug,
		dryrun:   *dryrun,
		priority: *priority,
		faclFile: *aclFile,
	}

	if flagSet.Changed("mode") {
		ctx.modeSet = true
		ctx.mode = *m
	}

	vals := flagSet.Args()
	dirsToMake := []*pathlib.Path{}
	if flagSet.Changed("file") {
		if flagSet.NArg() > 1 {
			err := fmt.Errorf("only 1 argument allowed")
			log.Error().Err(err).Msg("When using file mode, only one argument is allowed. Use --help for more info.")
			return Context{}, nil, err
		}
		dirsToMake, err = pkg.GetPathsFromFile(vals[0], *file)
		if err != nil {
			return Context{}, nil, err
		}
	} else {
		for i := 0; i < len(vals); i++ {
			dirsToMake = append(dirsToMake, pathlib.NewPath(vals[i]))
		}
	}
	return ctx, dirsToMake, nil
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

	log.Info().Msg("Starting Mkdir")
	log.Debug().Msg("Debug Mode")
	// CHANGE OVERRIDE TO FALSE BEFORE DEPLOY
	if err = launchMkdir(ctx, dirPaths); err != nil {
		log.Error().Err(err).Msg("Launch rsync failed")
		return err
	}
	return nil
}
