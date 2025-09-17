package main

import (
	"context"
	"fmt"
	_ "net/http/pprof"
	"os"
	"os/user"
	"strconv"

	pathlib "github.com/chigopher/pathlib"
	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/pkg"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog/log"
	flag "github.com/spf13/pflag"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

var CVMFS_RSYNC_VERSION = "unstable"
var CVMFS_RSYNC_SERVICE_NAME = "cvmfs_rsync"
var OVERRIDE_CONFIG = "false"
var OVERRIDE_CONFIG_FLAG_SET = false
var OVERRIDE_CONFIG_PATH = ""
var HOSTNAME = ""
var USERNAME = ""

var tr = otel.GetTracerProvider().Tracer(CVMFS_RSYNC_SERVICE_NAME)

type Context struct {
	cfg                       pkg.ConfStruct
	recursive                 bool
	dirs                      bool
	relative                  bool
	delete                    bool
	purge                     bool
	checksum                  bool
	linkDeref                 bool
	acls                      pkg.ACLFlag
	changelog                 *pathlib.Path
	dryrun                    bool
	debug                     bool
	exclude                   bool
	retryChangedFiles         bool
	excludeStrs               []string
	uidString                 string
	uid                       int
	groupIdMap                map[int]bool
	hostname                  string
	bu                        string
	filewalkHasher            *pkg.Hasher
	uploadHasher              *pkg.Hasher
	coreAllotment             int
	numWorkers                int
	numUploadHashers          int
	numFilewalkHashers        int
	numConcurrentUploaders    int
	numIOFilewalkWorkers      int
	numComputeFilewalkWorkers int
	skipGraft                 bool
	cvmfsChunkSize            int64
	trCtx                     context.Context
	pprofWebserver            bool
	numCpus                   int
	priority                  string
	filewalkCompressor        *pkg.Compressor
	uploadCompressor          *pkg.Compressor
	telegrafAddr              string
}

var onlyOneInCvmfs = pkg.OnlyOneInCvmfs
var getRepoPath = pkg.GetRepoPath
var getCvmfsConfInfo = pkg.GetCvmfsConfigurationInfo

// Processes the passed in src and dest paths and returns:
// localSrc, srcPaths, destRelative, destGhostPath, repo
func setupPathObjectsForRsync(srcPathStrings []string, destPathString string) (bool, []*pathlib.Path, *pathlib.Path, *pathlib.Path, *pathlib.Path, error) {
	// Process multiple srcs
	srcPaths := []*pathlib.Path{}
	for _, srcPathString := range srcPathStrings {
		srcPaths = append(srcPaths, pathlib.NewPath(srcPathString))
	}
	// Get the destPath from its string (real and ghost)
	destPath := pathlib.NewPath(destPathString)
	destRealPath, destGhostPath, err := pkg.LongestRealPath(destPath)
	if err != nil {
		return false, nil, nil, nil, nil, err
	}
	// Ensure only src or dest is in cvmfs
	onlyOne, localSrc, err := onlyOneInCvmfs(srcPaths, destRealPath)
	if err != nil {
		return false, nil, nil, nil, nil, err
	}
	if !onlyOne {
		err := fmt.Errorf("exactly one (src or dest) should be in cvmfs")
		log.Error().Err(err).Msg("Exactly one")
		return false, nil, nil, nil, nil, err
	}
	// Get the repo being copied to
	repo, destRelative, err := getRepoPath(destRealPath)
	if err != nil {
		return false, nil, nil, nil, nil, err
	}
	return localSrc, srcPaths, destRelative, destGhostPath, repo, nil
}

// Convert passed in args to rsync objects and call rsync processing function
func launchRsync(ctx Context, srcPathStrings []string, destPathString string) error {
	var err error
	if ctx.delete && !ctx.recursive && !ctx.dirs {
		err = fmt.Errorf("you are trying to do a delete without recursion, please specify -r or -d")
		log.Error().Err(err).Msg("Issue with flags")
		return err
	}
	if ctx.purge && !ctx.delete {
		err = fmt.Errorf("you are trying to do a purge without delete, please specify --delete")
		log.Error().Err(err).Msg("Issue with flags")
		return err
	}
	if ctx.purge && ctx.skipGraft {
		err = fmt.Errorf("you are trying to do a purge with skip graft, these are incompatible flags")
		log.Error().Err(err).Msg("Issue with flags")
		return err
	}
	if ctx.recursive && ctx.dirs {
		ctx.dirs = false
	}
	localSrc, srcPaths, destRelative, destGhostPath, repo, err := setupPathObjectsForRsync(srcPathStrings, destPathString)
	if err != nil {
		return err
	}

	ctx.cfg, ctx.uidString, ctx.uid, ctx.groupIdMap, err = getCvmfsConfInfo(repo.Name(), pkg.GetConfigFileForRepo(repo.Name(), OVERRIDE_CONFIG_FLAG_SET, OVERRIDE_CONFIG_PATH))
	if err != nil {
		log.Error().Err(err).Msg("Error parsing config")
		return err
	}
	if ctx.cfg.Repo.ContentAddressable {
		if ctx.purge {
			err := fmt.Errorf("config error")
			log.Error().Err(err).Msg("You cannot purge in a content addressable repository")
			return err
		}
		if ctx.cfg.Repo.DotScheme {
			err := fmt.Errorf("config error")
			log.Error().Err(err).Msg("A repo should not be dot scheme and content addressable. Please contact an administrator to discuss repo configuration")
			return err
		}
	}
	ctx.filewalkHasher = pkg.NewHasher(ctx.numFilewalkHashers, pkg.IOBufferSize)
	if ctx.cfg.Repo.ContentAddressable {
		ctx.cvmfsChunkSize = pkg.CVMFSInternalChunkSize
		ctx.filewalkCompressor = pkg.NewZlibCompressor(pkg.IOBufferSize)
	} else {
		ctx.cvmfsChunkSize = pkg.CVMFSChunkSize
		ctx.filewalkCompressor = nil
	}

	if localSrc {
		if err := rsync(ctx, srcPaths, destRelative, destGhostPath, repo, destPathString); err != nil {
			return err
		}
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

	// Do we still want these max values?
	newCtx.numIOFilewalkWorkers = min(numAllotedCpus*pkg.IOFilewalkThreadScalar, pkg.IOFilewalkThreadMax)
	newCtx.numComputeFilewalkWorkers = min(numAllotedCpus*pkg.ComputeFilewalkThreadScalar, pkg.ComputeFilewalkThreadMax)
	if ctx.checksum || ctx.dryrun {
		// For checksum autotuning, I am assuming that anyone using checksum will be using small files.
		newCtx.numFilewalkHashers = pkg.HasherUploaderAmount
		logLine = logLine.Int("hashers per compute worker", newCtx.numFilewalkHashers)
	}
	newCtx.numConcurrentUploaders = pkg.HasherUploaderAmount
	newCtx.numUploadHashers = pkg.HasherUploaderAmount
	newCtx.numWorkers = min(numAllotedCpus*pkg.WorkerScalar, pkg.WorkerMax)
	logLine.Int("num io workers", newCtx.numIOFilewalkWorkers).Int("num compute workers", newCtx.numComputeFilewalkWorkers).Int("num upload workers", newCtx.numWorkers).Int("upload hashers per worker", newCtx.numUploadHashers).Int("uploaders per worker", newCtx.numConcurrentUploaders).Msg("rsync using these parameters.")
	log.Info().Int("Allotted Cores", numAllotedCpus).Msg("Cvmfs rsyncing with these parameters")
	return newCtx
}

func setUserHostGlobals(ctx Context) {
	HOSTNAME = ctx.hostname
	uid := os.Getuid()
	user, err := user.LookupId(strconv.Itoa(uid))
	if err != nil {
		log.Error().Err(err).Msg("Error funding user info")
		USERNAME = "USER_LOOKUP_ERROR"
	} else {
		USERNAME = user.Username
	}
}

func getInfoFromFlags() (Context, []string, string, error) {
	flagSet := flag.NewFlagSet("flags", flag.ExitOnError)

	// Define help for tool
	flagSet.Usage = func() {
		fmt.Fprintf(os.Stderr, "\nThis is the cvmfs_rsync tool, meant to implement rsync functionality for the CVMFS file "+
			"system.\n\nUsage: cvmfs_rsync [OPTION] ... SRC DEST\nEither SRC or DEST must be in CVMFS, but not both.\n\nA special "+
			"note about rsync. When copying directories, path/to/dir will copy the directory as is into DEST,\nwhereas "+
			"/path/to/dir/ will only copy the directory contents into DEST\n\n"+
			"Memory Estimation:\n"+
			"To estimate the total working memory necessary for your rsync, the following formula will give you a good estimation in practice:\n"+
			"total_memory = 48MB * core-allotment\n"+
			"\n\n\n\nOptions\n")
		flagSet.PrintDefaults()
	}

	// Define all of the flags to be used by the tool
	r := flagSet.BoolP("recursive", "r", false, "Perform cvmfs_rsync recursively into SRC.")
	d := flagSet.BoolP("dirs", "d", false, "Perform cvmfs_rsync transferring dirs without recursing.")
	R := flagSet.BoolP("relative", "R", false, "Perform cvmfs_rsync with relative path names.")
	delete := flagSet.Bool("delete", false, "Removes files from DEST that are not present in SRC. By default no files will be removed.")
	purge := flagSet.Bool("purge", false, "Remove corresponding objects when files are deleted. Requires --delete.")
	c := flagSet.BoolP("checksum", "c", false, "Compare using a full file checksum rather than modtime + size.")
	L := flagSet.BoolP("copy-links", "L", false, "Dereference symlinks when copying from non-CVMFS to CVMFS.")
	var aclFlag = pkg.ACLNone
	flagSet.VarP(&aclFlag, "acls", "a", "ACL preservation: preserve-all (previous behaviour), preserve-mode, preserve-execute, preserve-owner, none (default)")
	dryrun := flagSet.BoolP("dry-run", "n", false, "Report on changes that would be made without uploading objects or making changes to CVMFS.")
	clog := flagSet.String("changelog", "", "Takes a <file> argument. Create a log file containing structured data for all changes made in <insert_format>.")
	exclude := flagSet.StringSlice("exclude", []string{}, "Exclude files matching one or more shell file name patterns i.e. --exclude=<pattern> --exclude=<pattern>. Patterns are applied in order from left to right. Quote <pattern> to avoid shell expansion prior to argument processing.")
	debug := flagSet.Bool("debug", false, "Sets log level to debug.")
	retryChangedFiles := flagSet.Bool("retry-changed-files", true, "Allows files to be re-uploaded if they change during the rsync run")
	flagSet.Int("num-io-fw-workers", pkg.IOFilewalkThreadScalar, "Sets the number of io bound file walk worker threads (By default this is tuned automatically, if you modify this flag, then the shown defaults will be used instead for fw)")
	flagSet.Int("num-compute-fw-workers", pkg.ComputeFilewalkThreadScalar, "Sets the number of compute bound file walk worker threads (By default this is tuned automatically, if you modify this flag, then the shown defaults will be used instead for fw)")
	flagSet.Int("channel-size", 10000000, "Sets the size of the file walk channels (the number of in-flight requests)")
	flagSet.Int("num-workers", pkg.WorkerScalar, "Sets the number of upload worker threads (By default this is tuned automatically, if you modify this flag, then the shown defaults will be used instead for upload)")
	flagSet.Int("num-hashers", pkg.HasherUploaderAmount, "Sets the number of hashers per upload thread (total possible threads = num-workers * max(num-hashers, num-concurrent-uploaders)) (By default this is tuned automatically, if you modify this flag, then the shown defaults will be used instead for upload)")
	flagSet.Int("num-concurrent-uploaders", pkg.HasherUploaderAmount, "Sets the number of s3-uploaders per upload thread (total possible threads = num-workers * max(num-hashers, num-concurrent-uploaders)) (By default this is tuned automatically, if you modify this flag, then the shown defaults will be used instead for upload)")
	coreAllotment := flagSet.Int("core-allotment", -1, "Automatically tunes your rsync run as if it had this number of cores alloted. Setting this value <1 instead (recommended) uses the number of cpus available as this value (maximum of 8, unless running in a slurm job which reads from slurm variable)")
	skipGraft := flagSet.Bool("skip-graft", false, "Skip grafting step and preserve graft db.")
	pprofWebserver := flagSet.Bool("run-profiling-webserver", false, "Run a webserver that runs a profiler.")
	priority := flagSet.StringP("priority", "P", pkg.LowPriority, "Priority of cvmfs_rsync job (low, med, high) (Note: In rare cases, low and med jobs can be serviced before high priority jobs). THIS REQUIRES AN UPGRADED GATEWAY TO USE. Please reach out to linux to get this set up, or if you are unsure if your gateway is upgraded.")
	file := flagSet.StringP("file", "f", "", "Perform the rsync from a line separated list of directories relative to a provided src directory (a la --files-from in traditional rsync). Ex. cvmfs_rsync --file=<file_of_paths> <src_root> <dest>. WARNING: Does not work for absolute paths, --relative flag will be based on file contents, not src root.")
	telegrafAddr := flagSet.String("telegraf-addr", pkg.DefaultTelegrafAddr, "The address to put telegraf stats to.")
	flagSet.MarkHidden("skip-graft")
	flagSet.MarkHidden("telegraf-addr")
	flagSet.MarkHidden("num-io-fw-workers")
	flagSet.MarkHidden("num-compute-fw-workers")
	flagSet.MarkHidden("num-workers")
	flagSet.MarkHidden("num-hashers")
	flagSet.MarkHidden("num-concurrent-uploaders")
	flagSet.MarkHidden("channel-size")

	err := flagSet.Parse(os.Args[1:])
	if err != nil {
		log.Error().Err(err).Msg("Flag parsing error")
		return Context{}, nil, "", err
	}

	if flagSet.NArg() < 2 {
		err := fmt.Errorf("at least 2 arguments needed")
		log.Error().Err(err).Msg("This tool needs 2 or more arguments, use --help for more info.")
		return Context{}, nil, "", err
	}

	// Initialize context with given flags
	ctx := Context{
		recursive:         *r,
		dirs:              *d,
		relative:          *R,
		delete:            *delete,
		purge:             *purge,
		checksum:          *c,
		linkDeref:         *L,
		acls:              aclFlag,
		dryrun:            *dryrun,
		debug:             *debug,
		coreAllotment:     *coreAllotment,
		retryChangedFiles: *retryChangedFiles,
		skipGraft:         *skipGraft,
		pprofWebserver:    *pprofWebserver,
		priority:          *priority,
		telegrafAddr:      *telegrafAddr,
	}

	if ctx.numIOFilewalkWorkers < 1 {
		ctx.numIOFilewalkWorkers = 1
	}
	if ctx.numComputeFilewalkWorkers < 1 {
		ctx.numComputeFilewalkWorkers = 1
	}

	if flagSet.Changed("changelog") {
		ctx.changelog = pathlib.NewPath(*clog)
	}
	if flagSet.Changed("exclude") {
		ctx.exclude = true
		ctx.excludeStrs = *exclude
	}

	// Parse non-flag arguments into src and dest paths
	vals := flagSet.Args()
	var srcPathStrings []string
	if flagSet.Changed("file") {
		if flagSet.NArg() > 2 {
			err := fmt.Errorf("only 2 arguments allowed")
			log.Error().Err(err).Msg("When using file mode, only two arguments are allowed, the src root and dest. Use --help for more info.")
			return Context{}, nil, "", err
		}
		srcPathStrings, err = pkg.GetPathStringsFromFile(*file)
		if err != nil {
			return Context{}, nil, "", err
		}
		err := os.Chdir(vals[0])
		if err != nil {
			log.Error().Err(err).Msg("Error setting up cvmfs_rsync from file")
			return Context{}, nil, "", err
		}
	} else {
		for i := 0; i < len(vals)-1; i++ {
			srcPathStrings = append(srcPathStrings, vals[i])
		}
	}
	destPathString := vals[len(vals)-1]
	return ctx, srcPathStrings, destPathString, nil
}

func Execute() error {
	if OVERRIDE_CONFIG == pkg.TrueString {
		OVERRIDE_CONFIG_FLAG_SET = true
	}
	pkg.SetupLogger()
	ctx, srcPathStrings, destPathString, err := getInfoFromFlags()
	if err != nil {
		log.Error().Err(err).Msg("Error parsing flags")
		return err
	}
	if ctx.debug {
		pkg.SetupDebugLogger()
	}

	setUserHostGlobals(ctx)

	ctx.trCtx = context.Background()
	shutdownTracer, err := pkg.InitTracer(ctx.trCtx, CVMFS_RSYNC_SERVICE_NAME, CVMFS_RSYNC_VERSION)
	if err != nil {
		log.Error().Err(err).Msg("Error setting up tracer")
	} else {
		defer func() {
			if err := shutdownTracer(ctx.trCtx); err != nil {
				log.Error().Err(err).Msg("Error when closing tracer")
			}
		}()
	}
	ctx.numCpus = pkg.SetMaxProcs()

	ctx = getAutotunedCtx(ctx)

	var span trace.Span
	ctx.trCtx, span = tr.Start(ctx.trCtx, "Execute")
	defer func() {
		log.Debug().Str("TraceID", span.SpanContext().TraceID().String()).Msg("Trace info")
		span.End()
	}()

	if ctx.pprofWebserver {
		go func() {
			if err := pkg.SetupPprofWebserver(); err != nil {
				os.Exit(1)
			}
		}()
	}

	log.Info().Msg("Starting Rsync")
	log.Debug().Msg("Debug Mode")
	// CHANGE OVERRIDE TO FALSE BEFORE DEPLOY
	if err = launchRsync(ctx, srcPathStrings, destPathString); err != nil {
		log.Error().Err(err).Msg("Launch rsync failed")
		return err
	}
	return nil
}
