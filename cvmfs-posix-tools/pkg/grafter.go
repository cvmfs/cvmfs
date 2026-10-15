package pkg

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type GraftOptions struct {
	PriorityVal int64
	Debug       bool
	Priority    string
}

// Converts the priority value to the associated priority string, lease retry interval, and start offset
func PriorityToPriorityInfo(priority string) (string, int64) {
	priorityVal := LowPriorityVal
	switch priority {
	case HighPriority:
		priorityVal = HighPriorityVal
	case MedPriority:
		priorityVal = MedPriorityVal
	case LowPriority:
		fallthrough
	default:
		priorityVal = LowPriorityVal
	}
	return priority, int64(priorityVal*1.e6) - time.Now().Unix()
}

type GraftMetrics struct {
	Delta                         float64
	Files, Dirs, Links, Deletions int
	Priority                      string
	LeasePath                     string
	Revision                      string
}

// Perform a graft with the passed in db using the currently loaded cvmfs_swissknife module
func NewGraftOptions(priority string) GraftOptions {
	// initialize the option with sensitive default
	priorityString, priorityVal := PriorityToPriorityInfo(priority)
	log.Info().Str("Priority", priorityString).Msg("Processing graft with the given priority.")
	return GraftOptions{
		PriorityVal: priorityVal,
		Debug:       false,
		Priority:    priorityString,
	}
}

func GraftWithOptions(db DB, repo string, options GraftOptions) (graftingMetrics GraftMetrics, err error) {
	numFiles, numDirs, numLinks, numDels, err := db.DBCounts()
	graftingMetrics = GraftMetrics{Files: numFiles, Dirs: numDirs, Links: numLinks, Deletions: numDels, Priority: options.Priority, LeasePath: MissingMetric, Revision: MissingMetric}
	stdoutExtractions := map[string]*regexp.Regexp{LeasePathRegexName: regexp.MustCompile(LeasePathRegex), RevisionRegexName: regexp.MustCompile(RevisionRegex)}
	stdoutExtracted := map[string][]string{}
	log.Info().Msg("Grafting:")
	start_time := time.Now()
	defer func() {
		end_time := time.Now()
		graftingMetrics.Delta = end_time.Sub(start_time).Seconds()
		log.Info().Float64("delta (s)", graftingMetrics.Delta).Msg("Grafting Time")
	}()
	var cvmfsRsyncTempDir string
	cvmfsRsyncTempDir, err = os.MkdirTemp("", "cvmfs_rsync_swissknife")
	if err != nil {
		return GraftMetrics{}, err
	}
	defer func() {
		if tempErr := os.RemoveAll(cvmfsRsyncTempDir); tempErr != nil {
			log.Error().Err(tempErr).Msg("Error in cleanup of tempDir")
			if err == nil {
				err = tempErr
			}
		}
	}()

	log.Debug().Int64("Priority Value", options.PriorityVal).Msg("Calculated Priority")

	if repo == "" {
		panic(fmt.Errorf("repo string is empty"))
	}

	var args = []string{
		"ingestsql",
		"-N", repo, /* fully qualified repository name */
		"-D", db.GetPath(), /* input sqlite DB */
		"-t", cvmfsRsyncTempDir, /* temporary directory */
		"-a",                                          /* Allow additions */
		"-d",                                          /* Allow deletions */
		"-B", fmt.Sprintf("/cvmfs/%s", repo),
		"-W", "61", /* seconds timeout for waiting */
		"-P", strconv.FormatInt(options.PriorityVal, 10),
	}

	cmd := exec.Command("cvmfs_swissknife", args...)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return GraftMetrics{}, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return GraftMetrics{}, err
	}
	// Get logging scanners
	stdoutIn := bufio.NewScanner(stdout)
	stderrIn := bufio.NewScanner(stderr)

	if err = cmd.Start(); err != nil {
		return GraftMetrics{}, err
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func(scanner *bufio.Scanner) {
		defer wg.Done()
		stdoutExtracted = LeveledPipeLoggerWithExtraction(scanner, zerolog.InfoLevel, stdoutExtractions)
	}(stdoutIn)
	wg.Add(1)
	go func(scanner *bufio.Scanner) {
		defer wg.Done()
		LeveledPipeLogger(scanner, zerolog.WarnLevel)
	}(stderrIn)

	wg.Wait()

	waitErr := cmd.Wait()
	ee, ok := waitErr.(*exec.ExitError)
	stderrString := ""
	if ok {
		stderrString = string(ee.Stderr)
	}

	exitcode := cmd.ProcessState.ExitCode()
	switch exitcode {
	case -1:
		// Was terminated by a signal.
		// Probably SIGABRT due to assertion failure,
		// or SIGSEGV due to memory access violation;
		// none of that is acceptable, even if shouldError==true.
		panic(fmt.Errorf("ingestsql command terminated (exit code %d) due to signal. Stderr:\n%s\n", exitcode, stderrString))
	case 0:
		// success
		break
	default:
		log.Error().Err(fmt.Errorf("ingestsql command failed (exit code %d). Stderr:\n%s\n", exitcode, stderrString)).Msg("")
		return GraftMetrics{}, waitErr
	}

	if match, ok := stdoutExtracted[LeasePathRegexName]; ok {
		graftingMetrics.LeasePath = match[1]
	}
	if match, ok := stdoutExtracted[RevisionRegexName]; ok {
		graftingMetrics.Revision = match[1]
	}

	log.Info().Msg("Finished Grafting")

	time.Sleep(PreSyncDelaySeconds * time.Second)

	return GraftMetrics{}, nil
}

func Graft(db DB, repo string, priority string, debug bool) (graftingMetrics GraftMetrics, err error) {
	options := NewGraftOptions(priority)
	options.Debug = debug
	return GraftWithOptions(db, repo, options)
}

// Remount and sync a repository (currently doesn't actually ensure sync afterwards)
func RemountSyncRepo(repo string, debug bool) error {
	cmd := exec.Command("sudo", "-u", "cvmfs", "/bin/cvmfs_talk", "-i", repo, "remount", "sync")

	if debug {
		cmd.Stdout = log.Logger
		cmd.Stderr = log.Logger
	}

	if err := cmd.Start(); err != nil {
		return err
	}
	if err := cmd.Wait(); err != nil {
		return err
	}

	return nil
}
