package main

import (
	"fmt"
	"os"

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
	cfg            pkg.ConfStruct
	hasher         pkg.SimpleHasher
	numHashers     int
	numCpus        int
	coreAllotment  int
	noDeref        bool
	debug          bool
	uid            int
	groupIdMap     map[int]bool
	cvmfsChunkSize int64
	priority       string
	compressor     *pkg.Compressor
}

func touch(ctx Context, paths []*pathlib.Path, repo *pathlib.Path) (err error) {
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

	if err = processTouch(ctx, paths, repo, db); err != nil {
		return err
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
var getHasher = pkg.GetSha1Hasher

func launchTouch(ctx Context, filePaths []*pathlib.Path) error {
	var err error
	// Get the destPath from its string (real and ghost)
	relPaths := []*pathlib.Path{}
	var repo *pathlib.Path
	firstDir := true
	for _, filePath := range filePaths {
		if filePath.Name() == pkg.CVMFSProtectedFile || filePath.Name() == pkg.CVMFSAutoProtectedFile {
			err = fmt.Errorf("protected file mod")
			log.Warn().Str("Protected File", filePath.String()).Msg("Refusing to modify cvmfs catalog")
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

	ctx.cfg, _, ctx.uid, ctx.groupIdMap, err = getCvmfsConfInfo(repo.Name(), pkg.GetConfigFileForRepo(repo.Name(), OVERRIDE_CONFIG_FLAG_SET, OVERRIDE_CONFIG_PATH))
	if err != nil {
		return err
	}

	if ctx.cfg.Repo.ContentAddressable {
		ctx.cvmfsChunkSize = pkg.CVMFSInternalChunkSize
		ctx.compressor = pkg.NewZlibCompressor(pkg.IOBufferSize)
	} else {
		ctx.cvmfsChunkSize = pkg.CVMFSChunkSize
		ctx.compressor = nil
	}
	ctx.hasher = getHasher(ctx.numHashers, ctx.cvmfsChunkSize, ctx.cfg.Repo.ContentAddressable)

	if err = touch(ctx, relPaths, repo); err != nil {
		log.Error().Err(err).Msg("Touch processing had a fatal error")
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

func getAutotunedCtx(ctx Context) Context {
	logLine := log.Debug() // Autotuning is now hidden without debug. We can deduce easily from number of cores.
	newCtx := ctx
	numAllotedCpus := ctx.numCpus
	if ctx.coreAllotment > 0 {
		numAllotedCpus = ctx.coreAllotment
	}

	newCtx.numHashers = min(numAllotedCpus*pkg.IOFilewalkThreadScalar, pkg.IOFilewalkThreadMax)
	logLine.Int("num purge workers", newCtx.numHashers).Msg("touch using these parameters")
	log.Info().Int("Allotted Cores", numAllotedCpus).Msg("Cvmfs touching with these parameters")
	return newCtx
}

func getInfoFromFlags() (Context, []*pathlib.Path, error) {
	flagSet := flag.NewFlagSet("flags", flag.ExitOnError)

	flagSet.Usage = func() {
		fmt.Fprintf(os.Stderr, "\nThis is the cvmfs_touch tool, meant to implement touch functionality for the CVMFS file "+
			" system for existing files. This tool will not create an empty file, making it different than traditional touch."+
			"\n\nUsage: cvmfs_touch [OPTION]... FILE...\nTool must be called in a CVMFS directory.\n\n"+
			"Memory Estimation:\n"+
			"To estimate the total working memory necessary for your insert, the following formula will give you a good estimation in practice:\n"+
			"total_memory = 30MB + 8MB * core-allotment\n\n"+
			"Generally this will be <1GB.\n"+
			"\n\n\n\nOptions\n")
		flagSet.PrintDefaults()
	}

	flagSet.Int("num-hashers", 30, "Sets the number of hashers")
	coreAllotment := flagSet.Int("core-allotment", -1, "Automatically tunes your rsync run as if it had this number of cores alloted. Setting this value <1 instead (recommended) uses the number of cpus available as this value (maximum of 8, unless running in a slurm job which reads from slurm variable)")
	noDeref := flagSet.BoolP("no-dereference", "h", false, "Affect any symlinks instead of the files they reference.")
	priority := flagSet.StringP("priority", "P", pkg.LowPriority, "Priority of cvmfs_rsync job (low, med, high) (Note: In rare cases, low and med jobs can be serviced before high priority jobs). THIS REQUIRES AN UPGRADED GATEWAY TO USE. Please reach out to linux to get this set up, or if you are unsure if your gateway is upgraded.")
	debug := flagSet.Bool("debug", false, "Add debug logging.")
	flagSet.MarkHidden("num-hashers")

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
		debug:         *debug,
		noDeref:       *noDeref,
		priority:      *priority,
		coreAllotment: *coreAllotment,
	}

	vals := flagSet.Args()
	filesToChange := []*pathlib.Path{}
	for i := 0; i < len(vals); i++ {
		filesToChange = append(filesToChange, pathlib.NewPath(vals[i]))
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

	ctx.numCpus = pkg.SetMaxProcs()
	ctx = getAutotunedCtx(ctx)

	log.Info().Msg("Starting touch")
	log.Debug().Msg("Debug Mode")
	// CHANGE OVERRIDE TO FALSE BEFORE DEPLOY
	if err = launchTouch(ctx, filePaths); err != nil {
		log.Error().Err(err).Msg("Launch touch failed")
		os.Exit(1)
	}
}
