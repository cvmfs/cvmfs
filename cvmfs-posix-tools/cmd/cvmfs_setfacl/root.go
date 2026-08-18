package main

import (
	"fmt"
	"os"
	"strings"

	pathlib "github.com/chigopher/pathlib"
	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/pkg"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog/log"
	flag "github.com/spf13/pflag"
)

var OVERRIDE_CONFIG = "false"
var OVERRIDE_CONFIG_FLAG_SET = false
var OVERRIDE_CONFIG_PATH = ""

type Context struct {
	cfg          pkg.ConfStruct
	recursive    bool
	uid          int
	debug        bool
	faclFile     string
	modifySet    bool
	removeSet    bool
	modifyString string
	removeAll    bool
	priority     string
}

func setfacl(ctx Context, paths []*pathlib.Path, repo *pathlib.Path) (err error) {
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

	if ctx.modifySet || ctx.removeSet || ctx.removeAll {
		// This allows users to specify just the group/user to remove that config from a facl
		if ctx.removeSet && (strings.Count(ctx.modifyString, ":") == 1) {
			ctx.modifyString = ctx.modifyString + ":--"
		}
		if err = processModSetfacl(ctx, ctx.modifyString, paths, repo, db); err != nil {
			return err
		}
	} else {
		var newAcl string
		newAcl, err = pkg.GetAclFromFile(ctx.faclFile)
		if err != nil {
			return err
		}

		if err = processNewSetfacl(ctx, newAcl, paths, repo, db); err != nil {
			return err
		}
	}

	dbEmpty, err := db.IsDatabaseEmpty()
	if err != nil {
		return err
	}
	if !dbEmpty {
		if _, err = graft(db, repo.Name(), ctx.priority, ctx.debug); err != nil {
			return err
		}
	} else {
		log.Info().Msg("Nothing to graft, skipping grafting step.")
	}

	return err
}

// Testing convenience function
var graft = pkg.Graft
var getRepoPath = pkg.GetRepoPath
var destInCvmfsFromFilePath = pkg.DestInCvmfsFromFilePath
var getCvmfsConfInfo = pkg.GetCvmfsConfigurationInfo

func launchSetfacl(ctx Context, filePaths []*pathlib.Path) error {
	var err error
	// Get the destPath from its string (real and ghost)
	relPaths := []*pathlib.Path{}
	var repo *pathlib.Path
	firstDir := true

	if (ctx.modifySet && ctx.removeSet) || (ctx.modifySet && ctx.removeAll) || (ctx.removeSet && ctx.removeAll) {
		return fmt.Errorf("some combo of modify, remove, and remove-all are set, you can only have one of these set at a time")
	}

	for _, filePath := range filePaths {
		if filePath.Name() == pkg.CVMFSProtectedFile || filePath.Name() == pkg.CVMFSAutoProtectedFile {
			err = fmt.Errorf("protected file mod")
			log.Warn().Str("Protected File", filePath.String()).Msg("Refusing to modify `.cvmfscatalog`")
			return err
		}
		filePath = filePath.Clean()
		// Ensure only src or dest is in cvmfs
		absPath, err := destInCvmfsFromFilePath(filePath)
		if err != nil {
			return err
		}

		// Get the repo being copied too
		var relPath *pathlib.Path
		if firstDir {
			repo, relPath, err = getRepoPath(absPath)
			if err != nil {
				return err
			}
		} else {
			relPath, err = absPath.RelativeTo(repo)
			if err != nil {
				err := fmt.Errorf("path is not relative to repository")
				log.Error().Err(err).Msg("You may be trying to upload to separate or non repositories. Please make sure every path is in the same cvmfs repo.")
			}
		}
		relPaths = append(relPaths, relPath)
	}

	ctx.cfg, _, ctx.uid, _, err = getCvmfsConfInfo(repo.Name(), pkg.GetConfigFileForRepo(repo.Name(), OVERRIDE_CONFIG_FLAG_SET, OVERRIDE_CONFIG_PATH))
	if err != nil {
		return err
	}

	if err = setfacl(ctx, relPaths, repo); err != nil {
		log.Error().Err(err).Msg("Setfacl processing had a fatal error")
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
		fmt.Fprintf(os.Stderr, "\nThis is the cvmfs_setfacl tool, meant to implement setfacl functionality for the CVMFS file "+
			" system.\n\nUsage: cvmfs_setfacl FACL-FILE DIRECTORY...\nTool must be called in a CVMFS directory.\n\n"+
			"Note: cvmfs does not store acls on files themselves. This tool will only modify the acls of directories, and skip other "+
			"file types.\n\n"+
			"Memory Estimation:\n"+
			"Generally this will be <1GB.\n"+
			"\n\n\n\nOptions\n")
		flagSet.PrintDefaults()
	}

	R := flagSet.BoolP("recursive", "R", false, "Change files and dirs recursively")
	debug := flagSet.Bool("debug", false, "Add debug logging.")
	file := flagSet.StringP("file", "f", "", "set the facl of the paths from a line separated list of paths relative to a provided root directory (a la cvmfs_insert). Ex. cvmfs_setfacl <facl_file> <repo_root> --file <path_file>")
	modify := flagSet.StringP("modify", "m", "", "modify the current ACL(s) of dir(s). Note, this replaces the need to specify a facl-file. E.x. cvmfs_setfacl -m <mod_string> <dirs>... Note: must be of the form <u,g>:<user or group>:<r/w/x>")
	remove := flagSet.StringP("remove", "x", "", "remove entries from the current ACL(s) of dir(s). Note, this replaces the need to specify a facl-file. E.x. cvmfs_setfacl -x <mod_string> <dirs>... Note: must be of the form <u,g>:<user or group>:---")
	removeAll := flagSet.BoolP("remove-all", "b", false, "remove all extended entries from the current ACL(s) of dir(s). Note, this replaces the need to specify a facl-file. E.x. cvmfs_setfacl -b <dirs>...")
	priority := flagSet.StringP("priority", "P", pkg.LowPriority, "Priority of cvmfs_rsync job (low, med, high) (Note: In rare cases, low and med jobs can be serviced before high priority jobs). THIS REQUIRES AN UPGRADED GATEWAY TO USE. Please reach out to linux to get this set up, or if you are unsure if your gateway is upgraded.")

	err := flagSet.Parse(os.Args[1:])
	if err != nil {
		log.Error().Err(err).Msg("Flag parsing error")
		return Context{}, nil, err
	}

	ctx := Context{
		recursive: *R,
		debug:     *debug,
		removeAll: *removeAll,
		priority:  *priority,
	}

	if flagSet.Changed("modify") {
		ctx.modifySet = true
		ctx.modifyString = *modify
	}

	if flagSet.Changed("remove") {
		ctx.removeSet = true
		ctx.modifyString = *remove
	}

	vals := flagSet.Args()

	fileStart := 1
	if ctx.modifySet || ctx.removeSet || ctx.removeAll {
		fileStart = 0
	} else {
		ctx.faclFile = vals[0]
	}

	if flagSet.NArg() < fileStart+1 {
		err := fmt.Errorf("at least %d argument(s) needed", fileStart+1)
		log.Error().Err(err).Msg("This tool needs a specific number of arguments, use --help for more info.")
		return Context{}, nil, err
	}

	filesToChange := []*pathlib.Path{}
	if flagSet.Changed("file") {
		if flagSet.NArg() > fileStart+1 {
			err := fmt.Errorf("only %d argument(s) allowed", fileStart+1)
			log.Error().Err(err).Msg("When using file mode, only specific arguments are allowed. Use --help for more info.")
			return Context{}, nil, err
		}
		filesToChange, err = pkg.GetPathsFromFile(vals[fileStart], *file)
		if err != nil {
			return Context{}, nil, err
		}
	} else {
		for i := fileStart; i < len(vals); i++ {
			filesToChange = append(filesToChange, pathlib.NewPath(vals[i]))
		}
	}
	return ctx, filesToChange, nil
}

func main() {
	if OVERRIDE_CONFIG == pkg.TrueString {
		OVERRIDE_CONFIG_FLAG_SET = true
	}
	pkg.SetupLogger()
	ctx, filePaths, err := getInfoFromFlags()
	if err != nil {
		log.Error().Err(err).Msg("Error parsing flags")
		os.Exit(1)
	}
	if ctx.debug {
		pkg.SetupDebugLogger()
	}

	log.Info().Msg("Starting Setfacl")
	log.Debug().Msg("Debug Mode")
	// CHANGE OVERRIDE TO FALSE BEFORE DEPLOY
	if err = launchSetfacl(ctx, filePaths); err != nil {
		log.Error().Err(err).Msg("Launch setfacl failed")
		os.Exit(1)
	}
}
