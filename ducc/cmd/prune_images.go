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
	pruneImagesDryRun      bool
	pruneImagesConfig      string
	pruneExpandedImageFiles []string
)

func init() {
	pruneImagesCmd.Flags().BoolVar(&pruneImagesDryRun, "dry-run", false,
		"Print what would be deleted without making any changes")
	pruneImagesCmd.Flags().StringVar(&pruneImagesConfig, "config", "",
		fmt.Sprintf("Path to a prune config file listing wishlist sources (default: %s in the current directory if present)",
			pruneImagesDefaultConfig))
	pruneImagesCmd.Flags().StringArrayVar(&pruneExpandedImageFiles, "expanded-images", nil,
		"Path to a YAML file containing a pre-expanded list of desired images (no wildcard expansion is performed); "+
			"may be specified multiple times")
	rootCmd.AddCommand(pruneImagesCmd)
}

var pruneImagesCmd = &cobra.Command{
	Use:   "prune-images <cvmfs-repo> [wishlist...]",
	Short: "Delete images from CVMFS that are not in the desired image set",
	Long: `Builds the complete desired set of images from one or more sources, then
removes from the CVMFS repository every image that is present on disk but
absent from that set.

Two kinds of input are accepted and may be freely combined:

  Wishlist files (positional args, --config, or config file "wishlists:" key)
    Standard ducc wishlist YAML files.  Wildcard tags are expanded by querying
    the registry, so the full concrete tag list is resolved at prune time.

  Pre-expanded image lists (--expanded-images flag or config "expanded_images:" key)
    A YAML file with an "images:" list of fully-resolved image references
    (no wildcards).  No registry queries are performed; the list is used as-is.
    Format:
      images:
        - https://registry.hub.docker.com/library/ubuntu:22.04
        - https://registry.hub.docker.com/library/ubuntu:20.04

The prune config file (--config / ducc-prune.yaml) format (YAML):

  wishlists:
    - /path/to/local/wishlist.yaml
    - https://example.com/wishlist.yaml
    - git+https://github.com/org/repo.git//path/to/wishlist.yaml@main
  expanded_images:
    - /path/to/expanded_images.yaml

Each wishlist source can be a local path, an HTTP/HTTPS URL, or a git URL:
  git+https://github.com/org/repo.git//path/to/wishlist.yaml[@branch]`,
	Args: cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		CVMFSRepo := args[0]
		wishlistSources := args[1:]

		pruneConfig, err := loadPruneConfig(pruneImagesConfig)
		if err != nil {
			return err
		}
		wishlistSources = append(wishlistSources, pruneConfig.Wishlists...)
		expandedImageFiles := append(pruneExpandedImageFiles, pruneConfig.ExpandedImages...)

		if len(wishlistSources) == 0 && len(expandedImageFiles) == 0 {
			return fmt.Errorf("no image sources provided: supply wishlist arguments, --expanded-images files, or a config file")
		}

		desiredImages := make(map[string]bool)

		// Expand wildcard wishlists against the registry.
		for _, source := range wishlistSources {
			images, err := expandWishlistToImages(source)
			if err != nil {
				l.LogE(err).WithFields(log.Fields{"source": source}).
					Error("Error expanding wishlist, continuing with others")
				continue
			}
			for _, img := range images {
				desiredImages[img.GetSimpleName()] = true
			}
		}

		// Load pre-expanded image lists directly (no registry queries).
		for _, path := range expandedImageFiles {
			images, err := loadExpandedImages(path)
			if err != nil {
				l.LogE(err).WithFields(log.Fields{"file": path}).
					Error("Error loading expanded images file, continuing with others")
				continue
			}
			for _, img := range images {
				desiredImages[img.GetSimpleName()] = true
			}
		}

		l.Log().WithFields(log.Fields{"desired_count": len(desiredImages)}).
			Info("Built desired image set, pruning CVMFS")

		deleted, err := lib.PruneImages(CVMFSRepo, desiredImages, pruneImagesDryRun)
		if err != nil {
			return err
		}

		if pruneImagesDryRun {
			fmt.Printf("[dry-run] Would delete %d image(s) from CVMFS\n", deleted)
		} else {
			fmt.Printf("Deleted %d image(s) from CVMFS\n", deleted)
		}
		return nil
	},
}

// PruneConfig is the schema for the ducc-prune.yaml config file.
type PruneConfig struct {
	Wishlists      []string `yaml:"wishlists"`
	ExpandedImages []string `yaml:"expanded_images"`
}

// loadPruneConfig reads the prune config file and returns its contents.
// If configPath is empty it looks for pruneImagesDefaultConfig in the current
// directory; if that file is also absent it returns an empty struct (not an error).
func loadPruneConfig(configPath string) (PruneConfig, error) {
	if configPath == "" {
		if _, err := os.Stat(pruneImagesDefaultConfig); os.IsNotExist(err) {
			return PruneConfig{}, nil
		}
		configPath = pruneImagesDefaultConfig
	}

	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return PruneConfig{}, fmt.Errorf("failed to read prune config %q: %w", configPath, err)
	}

	var cfg PruneConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return PruneConfig{}, fmt.Errorf("failed to parse prune config %q: %w", configPath, err)
	}

	l.Log().WithFields(log.Fields{
		"config":         configPath,
		"wishlists":      len(cfg.Wishlists),
		"expanded_images": len(cfg.ExpandedImages),
	}).Info("Loaded prune config")

	return cfg, nil
}

// expandWishlistToImages loads a wishlist file, expands all wildcard tags
// against the registry (via ParseYamlRecipeV1 / CreateWish), and returns the
// full concrete set of images the wishlist resolves to.
func expandWishlistToImages(source string) ([]*lib.Image, error) {
	data, err := loadWishlist(source)
	if err != nil {
		return nil, fmt.Errorf("failed to load wishlist %q: %w", source, err)
	}

	recipe, err := lib.ParseYamlRecipeV1(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse wishlist %q: %w", source, err)
	}

	var images []*lib.Image
	for wish := range recipe.Wishes {
		for _, img := range wish.ExpandedTagImagesLayer {
			images = append(images, img)
		}
	}
	return images, nil
}

// loadExpandedImages reads a pre-expanded image list from a YAML file and
// returns the parsed images.  The file must have an "images:" key whose value
// is a list of fully-resolved image references (no wildcard tags).  Any entry
// that still contains a wildcard tag is logged as a warning and skipped.
//
// Example file:
//
//	images:
//	  - https://registry.hub.docker.com/library/ubuntu:22.04
//	  - https://registry.hub.docker.com/library/ubuntu:20.04
func loadExpandedImages(path string) ([]*lib.Image, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read expanded images file %q: %w", path, err)
	}

	var file struct {
		Images []string `yaml:"images"`
	}
	if err := yaml.Unmarshal(data, &file); err != nil {
		return nil, fmt.Errorf("failed to parse expanded images file %q: %w", path, err)
	}

	var images []*lib.Image
	for _, ref := range file.Images {
		img, err := lib.ParseImage(ref)
		if err != nil {
			l.LogE(err).WithFields(log.Fields{"image": ref, "file": path}).
				Warning("Failed to parse image reference, skipping")
			continue
		}
		if img.TagWildcard {
			l.Log().WithFields(log.Fields{"image": ref, "file": path}).
				Warning("Expanded images file contains a wildcard tag — skipping; use a wishlist source for wildcard expansion")
			continue
		}
		images = append(images, &img)
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
