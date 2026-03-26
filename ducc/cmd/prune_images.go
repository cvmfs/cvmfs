package cmd

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"

	duccexec "github.com/cvmfs/ducc/exec"
	"github.com/cvmfs/ducc/lib"
	l "github.com/cvmfs/ducc/log"
)

const pruneImagesDefaultConfig = "ducc-prune.yaml"

var (
	pruneImagesDryRun bool
	pruneImagesConfig string
)

func init() {
	pruneImagesCmd.Flags().BoolVar(&pruneImagesDryRun, "dry-run", false,
		"Print what would be deleted without making any changes")
	pruneImagesCmd.Flags().StringVar(&pruneImagesConfig, "config", "",
		fmt.Sprintf("Path to a prune config file listing wishlist sources (default: %s in the current directory if present)",
			pruneImagesDefaultConfig))
	rootCmd.AddCommand(pruneImagesCmd)
}

var pruneImagesCmd = &cobra.Command{
	Use:   "prune-images <cvmfs-repo> [wishlist...]",
	Short: "Delete images from CVMFS that are no longer available on the registry",
	Long: `Reads one or more wishlist files, expands wildcard tags by querying the
registry, and removes from the CVMFS repository any image that the wishlist
covers but is no longer present on its source registry.

Wishlist sources are collected from two places (merged together):
  1. Positional arguments on the command line
  2. A prune config file (--config flag, or ducc-prune.yaml in the current directory)

The prune config file format (YAML):

  wishlists:
    - /path/to/local/wishlist.yaml
    - https://example.com/wishlist.yaml
    - git+https://github.com/org/repo.git//path/to/wishlist.yaml@main

Each wishlist source can be:
  - A local file path
  - An HTTP/HTTPS URL to a raw wishlist file
  - A git repository URL in the form:
      git+https://github.com/org/repo.git//path/to/wishlist.yaml
      git+https://github.com/org/repo.git//path/to/wishlist.yaml@branch`,
	Args: cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		CVMFSRepo := args[0]
		wishlistSources := args[1:]

		configSources, err := loadPruneConfig(pruneImagesConfig)
		if err != nil {
			return err
		}
		wishlistSources = append(wishlistSources, configSources...)

		if len(wishlistSources) == 0 {
			return fmt.Errorf("no wishlist sources provided: supply them as arguments or in a config file")
		}

		totalDeleted := 0
		for _, source := range wishlistSources {
			n, err := pruneFromWishlist(CVMFSRepo, source, pruneImagesDryRun)
			if err != nil {
				l.LogE(err).WithFields(log.Fields{"source": source}).
					Error("Error processing wishlist, continuing with others")
			}
			totalDeleted += n
		}

		if pruneImagesDryRun {
			fmt.Printf("[dry-run] Would delete %d image(s) from CVMFS\n", totalDeleted)
		} else {
			fmt.Printf("Deleted %d image(s) from CVMFS\n", totalDeleted)
		}
		return nil
	},
}

// PruneConfig is the schema for the ducc-prune.yaml config file.
type PruneConfig struct {
	Wishlists []string `yaml:"wishlists"`
}

// loadPruneConfig reads wishlist sources from the prune config file.
// If configPath is empty it looks for pruneImagesDefaultConfig in the current
// directory; if that file is also absent it returns an empty list (not an error).
func loadPruneConfig(configPath string) ([]string, error) {
	if configPath == "" {
		if _, err := os.Stat(pruneImagesDefaultConfig); os.IsNotExist(err) {
			return nil, nil
		}
		configPath = pruneImagesDefaultConfig
	}

	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read prune config %q: %w", configPath, err)
	}

	var cfg PruneConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse prune config %q: %w", configPath, err)
	}

	l.Log().WithFields(log.Fields{
		"config":    configPath,
		"wishlists": len(cfg.Wishlists),
	}).Info("Loaded wishlist sources from prune config")

	return cfg.Wishlists, nil
}

func pruneFromWishlist(CVMFSRepo, source string, dryRun bool) (int, error) {
	data, err := loadWishlist(source)
	if err != nil {
		return 0, fmt.Errorf("failed to load wishlist %q: %w", source, err)
	}

	images, err := parseWishlistInputs(data)
	if err != nil {
		return 0, fmt.Errorf("failed to parse wishlist %q: %w", source, err)
	}

	return lib.PruneImages(CVMFSRepo, images, dryRun)
}

// parseWishlistInputs reads the input image list from a wishlist YAML and
// returns the parsed Image values. The cvmfs_repo and output_format fields
// are intentionally ignored here since the repo is supplied on the command line.
func parseWishlistInputs(data []byte) ([]lib.Image, error) {
	var recipe struct {
		Input []string `yaml:"input"`
	}
	if err := yaml.Unmarshal(data, &recipe); err != nil {
		return nil, err
	}

	var images []lib.Image
	for _, inputStr := range recipe.Input {
		img, err := lib.ParseImage(inputStr)
		if err != nil {
			l.LogE(err).WithFields(log.Fields{"image": inputStr}).
				Warning("Failed to parse image from wishlist, skipping")
			continue
		}
		images = append(images, img)
	}
	return images, nil
}

// loadWishlist fetches wishlist content from a local file path, an HTTP/HTTPS
// URL, or a git repository URL (git+https://...).
func loadWishlist(source string) ([]byte, error) {
	switch {
	case strings.HasPrefix(source, "git+"):
		return loadWishlistFromGit(source)
	case strings.HasPrefix(source, "http://") || strings.HasPrefix(source, "https://"):
		return loadWishlistFromHTTP(source)
	default:
		return ioutil.ReadFile(source)
	}
}

func loadWishlistFromHTTP(url string) ([]byte, error) {
	resp, err := http.Get(url) // #nosec G107 — URL is user-supplied on CLI
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("HTTP error %d fetching %s", resp.StatusCode, url)
	}
	return ioutil.ReadAll(resp.Body)
}

// loadWishlistFromGit fetches a single file from a git repository without
// requiring a full clone of the history.
//
// URL format:
//
//	git+https://github.com/org/repo.git//path/to/wishlist.yaml
//	git+https://github.com/org/repo.git//path/to/wishlist.yaml@branch-or-tag
func loadWishlistFromGit(gitURL string) ([]byte, error) {
	// Strip the "git+" scheme prefix.
	repoAndFile := strings.TrimPrefix(gitURL, "git+")

	// Split on "//" to separate repository URL from file path within the repo.
	parts := strings.SplitN(repoAndFile, "//", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf(
			"invalid git URL %q: expected git+REPO_URL//path/to/file[@branch]", gitURL)
	}
	repoURL, filePart := parts[0], parts[1]

	// Optional @branch suffix on the file path.
	branch := ""
	if idx := strings.LastIndex(filePart, "@"); idx != -1 {
		branch = filePart[idx+1:]
		filePart = filePart[:idx]
	}

	tmpDir, err := ioutil.TempDir("", "ducc-git-")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp dir for git clone: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	cloneArgs := []string{"git", "clone", "--depth", "1"}
	if branch != "" {
		cloneArgs = append(cloneArgs, "--branch", branch)
	}
	cloneArgs = append(cloneArgs, repoURL, tmpDir)

	if err := duccexec.ExecCommand(cloneArgs...).Start(); err != nil {
		return nil, fmt.Errorf("failed to clone git repository %q: %w", repoURL, err)
	}

	filePath := filepath.Join(tmpDir, filepath.FromSlash(filePart))
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read %q from git repository: %w", filePart, err)
	}
	return data, nil
}
