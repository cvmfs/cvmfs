package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"os/user"
	"strconv"
	"sync"
	"time"

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
var HOSTNAME = ""
var USERNAME = ""

const (
	CSV_RECORD_TYPE_FIELD = iota
	CSV_RECORD_SRC_FIELD
	CSV_RECORD_DST_FIELD
	CSV_RECORD_FIELD_COUNT
)

const (
	RECORD_TYPE_UNKNOWN = iota
	RECORD_TYPE_INSERT
	RECORD_TYPE_FACL
)

type Context struct {
	cfg                    pkg.ConfStruct
	acls                   pkg.ACLFlag
	debug                  bool
	uid                    int
	uidString              string
	groupIdMap             map[int]bool
	hostname               string
	bu                     string
	numWorkers             int
	numHashers             int
	dryrun                 bool
	hasher                 *pkg.Hasher
	cvmfsChunkSize         int64
	skipGraft              bool
	priority               string
	retryChangedFiles      bool
	numConcurrentUploaders int
	numCpus                int
	coreAllotment          int
	noDeref                bool
	telegrafAddr           string
}

type ItemResult struct {
	err  error
	item ItemGeneric
}

type ItemGeneric interface {
	Populate(ctx *Context, repoPathStr string, src string, dst string) error
	GetDestPath() *pathlib.Path
	GetCanWriteHint() bool
	InsertGraft(fileCfg pkg.ConfStruct, uid int, noDeref bool, db *pkg.CvmfsDB) error
	MaybeGhost() bool
}

type GenericInput interface {
	GetIdx() int
}

var graft = pkg.GraftWithOptions
var getRepoPath = pkg.GetRepoPath
var getCvmfsConfInfo = pkg.GetCvmfsConfigurationInfo

func ProcessInsertions(
	ctx Context, db *pkg.CvmfsDB, repo *pathlib.Path, insertionResult []ItemResult) error {
	ghostPaths := make(map[string]bool)

	for _, result := range insertionResult {
		if result.err != nil {
			return result.err
		}

		item := result.item

		dstFileParentPath := item.GetDestPath().Parent()
		var fileCfg = pkg.GetBasePathPrefix(ctx.cfg, item.GetDestPath())

		var err error
		if !item.GetCanWriteHint() && !ghostPaths[dstFileParentPath.String()] {
			canWrite, err := pkg.UserCanWriteDir(
				fileCfg, dstFileParentPath, repo, true, ctx.groupIdMap, ctx.uid)
			if err != nil {
				return err
			}
			if !canWrite {
				err = fmt.Errorf("you do not have write permissions to path, quitting")
				log.Error().Err(err).Str(
					"Path", repo.JoinPath(dstFileParentPath).Clean().String()).Msg("Permission error")
				return err
			}
		}

		err = item.InsertGraft(fileCfg, ctx.uid, ctx.noDeref, db)
		if err != nil {
			return err
		}

		if item.MaybeGhost() {
			ghostPaths[item.GetDestPath().String()] = true
		}
	}
	return nil
}

func getAutotunedUploadCtx(ctx Context, avgFileSize int) Context {
	newCtx := ctx
	if pkg.SmallFileSizeHeuristic < avgFileSize && newCtx.numWorkers > 1 {
		newCtx.numConcurrentUploaders = max(newCtx.numConcurrentUploaders*2, 1)
		newCtx.numHashers = max(newCtx.numHashers*2, 1)
		newCtx.numWorkers = max(newCtx.numWorkers/2, 1)
		if pkg.LargeFileSizeHeuristic < avgFileSize && newCtx.numWorkers > 1 {
			newCtx.numConcurrentUploaders = max(newCtx.numConcurrentUploaders*2, 1)
			newCtx.numHashers = max(newCtx.numHashers*2, 1)
			newCtx.numWorkers = max(newCtx.numWorkers/2, 1)
		}
		log.Debug().Int("num workers", newCtx.numWorkers).Int("uploaders per worker", newCtx.numConcurrentUploaders).Int("hashers per worker", newCtx.numHashers).Msg("Re-Autotuned upload due to large file size.")
	}
	return newCtx
}

func DoInsert(ctx Context, repoPath string, insertions []ItemResult) (err error) {
	uploadStatistics := UploadStatistics{}
	var graftMetrics pkg.GraftMetrics
	revisionNum := pkg.MissingMetric
	revisionNumBytes, err := xattr.Get(repoPath, pkg.MountRevisionXattr)
	if err != nil {
		log.Error().Err(err).Msg("Error getting revision")
	} else {
		revisionNum = string(revisionNumBytes)
	}
	var db *pkg.CvmfsDB
	db, err = pkg.NewCvmfsGraftingDB()
	if err != nil {
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

	repo := pathlib.NewPath(repoPath)
	err = ProcessInsertions(ctx, db, repo, insertions)
	if err != nil {
		return err
	}

	ctx.hasher = pkg.NewHasher(ctx.numHashers, pkg.IOBufferSize)

	if fileAvgSize, err := db.QueryFilesAvgSize(); err != nil {
		log.Debug().Err(err).Msg("Unable to get file sizes, skipping optimization")
	} else {
		// Casting data loss is irrelevant to functionality here
		ctx = getAutotunedUploadCtx(ctx, int(fileAvgSize))
	}

	if !ctx.dryrun {
		var s3Interface S3Interface
		s3Interface, err = newBasicS3Manager(ctx)
		if err != nil {
			return err
		}
		var alternateS3Interface S3Interface
		alternateS3Interface, err = newAlternateS3Manager(ctx)
		if err != nil {
			return err
		}
		if uploadStatistics, err = uploadFiles(ctx, s3Interface, alternateS3Interface, db); err != nil {
			return err
		}
		start_time := time.Now()
		if nameClashes, err := db.FileNameClashes(); err != nil {
			return err
		} else if len(nameClashes) > 0 {
			err = fmt.Errorf("name clashes found with necessary cvmfs naming conventions")
			log.Error().Err(err).Strs("Clashing Names", nameClashes).Msg("Data loss would result from these name conflicts. Please resolve name conflicts to proceed with rsync")
			return err
		}
		end_time := time.Now()
		delta := end_time.Sub(start_time).Seconds()
		log.Info().Float64("delta (s)", delta).Msg("Name Clash Resolution Done")

		var dbEmpty bool
		if dbEmpty, err = db.IsDatabaseEmpty(); err != nil {
			return err
		} else if !dbEmpty {
			if ctx.skipGraft {
				log.Info().Msg("Skipping grafting step due to --skip-graft flag")
				db.BackupDatabase("graft.db")
			} else {
				opts := pkg.NewGraftOptions(ctx.priority)
				opts.Debug = ctx.debug
				if graftMetrics, err = graft(db, repo.Name(), opts); err != nil {
					return err
				}
				revisionNum = graftMetrics.Revision
			}
		} else {
			log.Info().Msg("Nothing changed, skipping Grafting step")
		}
	} else {
		log.Info().Msg("dry run: upload and grafting skipped.")
	}

	// Creates changelog and prints dryrun
	if err = pkg.CreateChangelog(ctx.dryrun, nil, db, revisionNum); err != nil {
		return err
	}

	pkg.SendTelegrafStatistics(TelegrafStats(ctx, repo.Name(), uploadStatistics, graftMetrics), ctx.telegrafAddr)

	return err
}

func TelegrafStats(ctx Context, repoName string, uploadStatistics UploadStatistics, graftMetrics pkg.GraftMetrics) string {
	// Core Allotment will be added in the future. It should be in these statistics.
	if graftMetrics.Priority == "" {
		graftMetrics.Priority = pkg.LowPriority
	}
	statisticsString := fmt.Sprintf("populateWorkers=%d,uploadHashers=%d,uploadUploaders=%d,coreAllotment=%d,numCpus=%d,uploadFileCount=%d,uploadDelta=%f,uploadRate=%f,uploadSize=%d,graftDelta=%f,numGraftFiles=%d,numGraftDirs=%d,numGraftLinks=%d,numGraftDeletions=%d",
		ctx.numWorkers, ctx.numHashers, ctx.numConcurrentUploaders, ctx.coreAllotment, ctx.numCpus, uploadStatistics.numFiles, uploadStatistics.delta, uploadStatistics.rate, uploadStatistics.totalSize, graftMetrics.Delta, graftMetrics.Files, graftMetrics.Dirs, graftMetrics.Links, graftMetrics.Deletions)
	return fmt.Sprintf("cvmfs_insert,user=%s,repo=%s,cvmfsRsyncVersion=%s,graftPriority=%s,lease_path=%s %s\n", USERNAME, repoName, CVMFS_RSYNC_VERSION, graftMetrics.Priority, graftMetrics.LeasePath, statisticsString)
}

func getAutotuneCtx(ctx Context) Context {
	newCtx := ctx
	numAllotedCpus := ctx.numCpus
	if ctx.coreAllotment > 0 {
		numAllotedCpus = ctx.coreAllotment
	}
	newCtx.numHashers = pkg.HasherUploaderAmount
	newCtx.numConcurrentUploaders = pkg.HasherUploaderAmount
	newCtx.numWorkers = min(numAllotedCpus*pkg.WorkerScalar, pkg.WorkerMax)
	log.Debug().Int("num hashers", newCtx.numHashers).Int("num hashers workers", newCtx.numWorkers).Int("num upload workers", newCtx.numWorkers).Int("uploaders per worker", newCtx.numConcurrentUploaders).Int("hashers per worker", newCtx.numHashers).Msg("insert using these parameters.")
	log.Info().Int("Allotted Cores", numAllotedCpus).Msg("Cvmfs inserting with these parameters")
	return newCtx
}

func LaunchInsert(ctx Context, repoPath string, inputCsvFile string) error {
	var err error
	repoName := pathlib.NewPath(repoPath).Name()
	ctx.cfg, ctx.uidString, ctx.uid, ctx.groupIdMap, err = getCvmfsConfInfo(
		repoName, pkg.GetConfigFileForRepo(repoName, OVERRIDE_CONFIG_FLAG_SET, OVERRIDE_CONFIG_PATH))
	if err != nil {
		log.Error().Err(err).Msg("Error parsing config")
		return err
	}

	if ctx.cfg.Repo.ContentAddressable && ctx.cfg.Repo.DotScheme {
		err := fmt.Errorf("config error")
		log.Error().Err(err).Msg("A repo should not be dot scheme and content addressable. Please contact an administrator to discuss repo configuration")
		return err
	}

	ctx.hasher = pkg.NewHasher(ctx.numHashers, pkg.IOBufferSize)
	if ctx.cfg.Repo.ContentAddressable {
		ctx.cvmfsChunkSize = pkg.CVMFSInternalChunkSize
	} else {
		ctx.cvmfsChunkSize = pkg.CVMFSChunkSize
	}

	f, err := os.Open(inputCsvFile)
	if err != nil {
		log.Error().Err(err).Msg("Unable to read input file " + inputCsvFile)
		return err
	}
	defer f.Close()

	csvReader := csv.NewReader(f)
	records, err := csvReader.ReadAll()
	if err != nil {
		log.Error().Err(err).Msg("Unable to parse csv input file " + inputCsvFile)
		return err
	}

	inputs := make(chan GenericInput, len(records))
	itemCount := 0
	for _, record := range records {
		if len(record) < CSV_RECORD_FIELD_COUNT {
			continue
		}
		if record[CSV_RECORD_TYPE_FIELD] == "insert" {
			inputs <- FileInput{
				idx: itemCount,
				src: record[CSV_RECORD_SRC_FIELD],
				dst: record[CSV_RECORD_DST_FIELD],
			}
		} else if record[CSV_RECORD_TYPE_FIELD] == "facl" {
			inputs <- FaclInput{
				idx:      itemCount,
				faclFile: record[CSV_RECORD_SRC_FIELD],
				dst:      record[CSV_RECORD_DST_FIELD],
			}
		} else {
			continue
		}
		itemCount++
	}
	close(inputs)

	insertions := make([]ItemResult, itemCount)

	ctx = getAutotuneCtx(ctx)
	ctx.hasher = pkg.NewHasher(ctx.numHashers, pkg.IOBufferSize)

	var wg sync.WaitGroup
	wg.Add(ctx.numWorkers)

	for i := 0; i < ctx.numWorkers; i++ {
		go func(
			in <-chan GenericInput,
			repoPath string,
			ctx *Context,
			items []ItemResult) {
			defer wg.Done()
			for inFile := range in {
				switch inputFile := inFile.(type) {
				case FileInput:
					var res = &items[inputFile.idx]
					res.item = &ItemInsert{}
					res.err = res.item.Populate(
						ctx,
						repoPath,
						inputFile.src,
						inputFile.dst)
				case FaclInput:
					var res = &items[inputFile.idx]
					res.item = &ItemFacl{}
					res.err = res.item.Populate(
						ctx,
						repoPath,
						inputFile.faclFile,
						inputFile.dst)
				default:
					continue
				}
			}
		}(inputs, repoPath, &ctx, insertions)
	}

	wg.Wait()

	if err = DoInsert(ctx, repoPath, insertions); err != nil {
		log.Error().Err(err).Msg("insert processing had a fatal error")
		return err
	}
	return nil
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

func getInfoFromFlags() (Context, string, string, error) {
	flagSet := flag.NewFlagSet("flags", flag.ExitOnError)

	flagSet.Usage = func() {
		fmt.Fprintf(os.Stderr, "\nThis is the cvmfs_insert tool, meant to implement bulk inserts for the CVMFS file "+
			"system.\n\nUsage: cvmfs_insert [OPTION]... CVMFS_REPO FILE\n\n"+
			"FILE is a CSV file containing the list of files to upload and the relative destinations in the CVMFS repository.\n\n"+
			"\nExample:\n"+
			"insert,<source path 1>,<relative dest path 1>\n"+
			"insert,<source path 2>,<relative dest path 2>\n"+
			"...\n"+
			"insert,<source path X>,<relative dest path Y>\n"+
			"\n\n"+
			"If the source path is a directory, a new directory will created if non-existant at destination"+
			"\n\n"+
			"FILE can also contain facl paths which will take in a path to a file describing an acl (similar to cvmfs_setfacl) and a directory and\n"+
			"apply that facl to that directory.\n\n"+
			"\nExample:\n"+
			"facl,<facl path 1>,<dest path 1>\n"+
			"facl,<facl path 2>,<dest path 2>\n"+
			"...\n"+
			"facl,<facl path X>,<dest path Y>\n\n"+
			"NOTE: facl ONLY works with dirs. The --no-dereference flag does NOT work for this feature.\n"+
			"Duplicate dest entries will take the FIRST acl file supplied for that destination.\n"+
			"\n\n"+
			"Memory Estimation:\n"+
			"To estimate the total working memory necessary for your insert, the following formula will give you a good estimation in practice:\n"+
			"total_memory = 48MB * core-allotment\n"+
			"\n\nOptions\n")
		flagSet.PrintDefaults()
	}

	flagSet.Int("num-hashers", pkg.HasherUploaderAmount, "Sets the number of hashers")
	flagSet.Int("num-workers", pkg.WorkerScalar, "Sets the number of worker threads")
	flagSet.Int("num-concurrent-uploaders", pkg.HasherUploaderAmount, "Sets the number of s3-uploaders per upload thread (total possible threads = num-workers * max(num-hashers, num-concurrent-uploaders))")
	coreAllotment := flagSet.Int("core-allotment", -1, "Automatically tunes your insert run as if it had this number of cores alloted. Setting this value <1 instead (recommended) uses the number of cpus available as this value (maximum of 8, unless running in a slurm job which reads from slurm variable)")
	skipGraft := flagSet.Bool(
		"skip-graft", false, "Skip grafting step and preserve graft db.")
	retryChangedFiles := flagSet.Bool(
		"retry-changed-files",
		true,
		"Allows files to be re-uploaded if they change during the insert run")
	debug := flagSet.Bool("debug", false, "Add debug logging.")
	dryrun := flagSet.BoolP(
		"dry-run",
		"n",
		false,
		"Report on changes that would be made without uploading objects or making changes to CVMFS.")
	var aclFlag = pkg.ACLNone
	flagSet.VarP(
		&aclFlag,
		"acls",
		"a",
		"ACL preservation: preserve-all (previous behaviour), preserve-mode, preserve-execute, preserve-owner, none (default)")
	noDeref := flagSet.BoolP("no-dereference", "N", false, "Do not dereference the final path component in ln processing (allows for symlinks pointing to dirs to be changed)")

	telegrafAddr := flagSet.String("telegraf-addr", pkg.DefaultTelegrafAddr, "The address to put telegraf stats to.")
	flagSet.MarkHidden("num-workers")
	flagSet.MarkHidden("num-hashers")
	flagSet.MarkHidden("num-concurrent-uploaders")
	priority := flagSet.StringP("priority", "P", pkg.LowPriority, "Priority of cvmfs_rsync job (low, med, high) (Note: In rare cases, low and med jobs can be serviced before high priority jobs). THIS REQUIRES AN UPGRADED GATEWAY TO USE. Please reach out to linux to get this set up, or if you are unsure if your gateway is upgraded.")

	err := flagSet.Parse(os.Args[1:])
	if err != nil {
		log.Error().Err(err).Msg("Flag parsing error")
		return Context{}, "", "", err
	}

	if flagSet.NArg() < 2 {
		err := fmt.Errorf("at least 2 argument needed")
		log.Error().Err(err).Msg("This tool needs 1 or more arguments, use --help for more info.")
		return Context{}, "", "", err
	}

	ctx := Context{
		debug:             *debug,
		dryrun:            *dryrun,
		priority:          *priority,
		acls:              aclFlag,
		skipGraft:         *skipGraft,
		retryChangedFiles: *retryChangedFiles,
		coreAllotment:     *coreAllotment,
		noDeref:           *noDeref,
		telegrafAddr:      *telegrafAddr,
	}

	if ctx.numWorkers < 1 {
		ctx.numWorkers = 1
	}

	vals := flagSet.Args()
	return ctx, vals[0], vals[1], nil
}

func Execute() error {
	if OVERRIDE_CONFIG == pkg.TrueString {
		OVERRIDE_CONFIG_FLAG_SET = true
	}
	pkg.SetupLogger()
	ctx, repo, inputFile, err := getInfoFromFlags()
	if err != nil {
		log.Error().Err(err).Msg("Error parsing flags")
		return err
	}
	if ctx.debug {
		pkg.SetupDebugLogger()
	}
	ctx.numCpus = pkg.SetMaxProcs()

	setUserHostGlobals(ctx)

	repo, _, err = resolveRepo(repo)
	if err != nil {
		log.Error().Err(err).Msg("Can't resolve repository at " + repo)
		return err
	}

	log.Info().Msg("Starting insert")
	log.Debug().Msg("Debug Mode")
	if err = LaunchInsert(ctx, repo, inputFile); err != nil {
		log.Error().Err(err).Msg("Launch insert failed")
		return err
	}
	return nil
}
