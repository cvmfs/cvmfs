package pkg

import (
	"fmt"
	"os"
	"os/exec"

	pathlib "github.com/chigopher/pathlib"
	"github.com/rs/zerolog/log"
)

// Get a mock grafter (Needs to match graft function signature)
func Mock_graft_getter() func(db DB, repo string, priority string, debug bool) (GraftMetrics, error) {
	absPath, err := GetAbsolutePath(pathlib.NewPath(CurrentDirectory))
	if err != nil {
		panic(err)
	}
	return func(db DB, repo string, priority string, debug bool) (GraftMetrics, error) {
		return Mock_graft(db, repo, debug, absPath.Parent().Parent().Clean().String())
	}
}

func Mock_graft_getter_options() func(db DB, repo string, options GraftOptions) (GraftMetrics, error) {
	absPath, err := GetAbsolutePath(pathlib.NewPath(CurrentDirectory))
	if err != nil {
		panic(err)
	}
	return func(db DB, repo string, options GraftOptions) (GraftMetrics, error) {
		return Mock_graft(db, repo, options.Debug, absPath.Parent().Parent().Clean().String())
	}
}

// Perform a graft to the test mount
func Mock_graft(db DB, repo string, debug bool, baseDir string) (GraftMetrics, error) {
	log.Info().Msg("Grafting:")
	cvmfsRsyncTempDir, err := os.MkdirTemp("", "cvmfs_rsync_swissknife")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(cvmfsRsyncTempDir)
	if repo == "" {
		repo = TestMountName()
	}
	configPrefix := "/etc/cvmfs/gateway-client/" + repo + "/"
	args_add := []string{
		"-B", fmt.Sprintf("/cvmfs/%s", repo),
		"-W", "61", /* seconds timeout for waiting */
	}
	if !FullyContainerized() {
		configPrefix = baseDir + "/pkg/etc/cvmfs-gateway-client/" + repo + "/"
		args_add = []string{}
	}
	args := append([]string{"ingestsql", "-N", repo,
		"-D", db.GetPath(), "-w", "http://127.0.0.1:9000/mybucket/" + repo, "-k", configPrefix + "pubkey",
		"-s", configPrefix + "gatewaykey", "-3", configPrefix + "s3.conf", "-g", "http://127.0.0.1:4929/api/v1",
		"-t", cvmfsRsyncTempDir, "-a", "-d"}, args_add...)

	cmd := exec.Command("cvmfs_swissknife", args...)

	fmt.Println(cmd.String())

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stdout
	if err := cmd.Start(); err != nil {
		panic(err)
	}

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
	log.Info().Msg("Finished Grafting")

	return GraftMetrics{}, nil
}

// Mock it out so that if graft is called, an error is thrown
func Mock_no_graft(db DB, repo string, priority string, debug bool) (GraftMetrics, error) {
	panic(fmt.Errorf("nothing should be grafted so this shouldn't be called"))
}

// Mock it out so that if graft is called, an error is thrown
func Mock_no_graft_options(db DB, repo string, options GraftOptions) error {
	panic(fmt.Errorf("nothing should be grafted so this shouldn't be called"))
}

// Return a grafting function that doesn't graft, but instead copies the databse into another database to be grafted later
func Mock_mega_graft(graftDb DB) func(db DB, repo string, priority string, debug bool) (GraftMetrics, error) {
	return func(db DB, repo string, priority string, debug bool) (GraftMetrics, error) {
		log.Info().Msg("Grafting:")
		err := graftDb.CopyInDatabase(db)
		if err != nil {
			return GraftMetrics{}, err
		}
		log.Info().Msg("Finished Grafting")
		return GraftMetrics{}, nil
	}
}

// Return a grafting function that doesn't graft, but instead copies the databse into another database to be grafted later
func Mock_mega_graft_options(graftDb DB) func(db DB, repo string, options GraftOptions) (GraftMetrics, error) {
	return func(db DB, repo string, options GraftOptions) (GraftMetrics, error) {
		log.Info().Msg("Grafting:")
		err := graftDb.CopyInDatabase(db)
		if err != nil {
			return GraftMetrics{}, err
		}
		log.Info().Msg("Finished Grafting")
		return GraftMetrics{}, nil
	}
}
