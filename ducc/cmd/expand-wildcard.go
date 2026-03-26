package cmd

import (
	"fmt"
	"io/ioutil"
	"sync"

	"github.com/cvmfs/ducc/lib"
	l "github.com/cvmfs/ducc/log"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

var (
	wishlistFiles []string
	outputFile    string
)

func init() {
	expandWildcardCmd.Flags().StringArrayVarP(&wishlistFiles, "wishlist", "w", nil,
		"path to a wishlist YAML file; may be specified multiple times to merge several wishlists")
	expandWildcardCmd.Flags().StringVarP(&outputFile, "output", "o", "",
		"write the expanded image list in YAML format to this file (suitable for prune-images --expanded-images); "+
			"requires --wishlist")
	rootCmd.AddCommand(expandWildcardCmd)
}

var expandWildcardCmd = &cobra.Command{
	Use:   "expand-wildcard [image]",
	Short: "List all the tags currently accessible under the image string",
	Long: `Expands wildcard image references and prints the resulting concrete tags.

Two modes of operation:

  Single-image mode (positional argument)
    Provide one image reference on the command line.  If the tag contains a
    wildcard the registry is queried and all matching tags are printed, one per
    line.  For a concrete tag the manifest is fetched to verify it exists.

  Wishlist mode (--wishlist / -w)
    Provide one or more wishlist YAML files.  All images in every file are
    expanded and the complete flat list is emitted.  Use --output / -o to write
    the result as a YAML file that prune-images --expanded-images can consume
    directly instead of printing to stdout.`,
	Args: func(cmd *cobra.Command, args []string) error {
		if cmd.Flags().Changed("wishlist") {
			if len(args) > 0 {
				return fmt.Errorf("positional image argument cannot be used together with --wishlist")
			}
			return nil
		}
		if cmd.Flags().Changed("output") {
			return fmt.Errorf("--output requires --wishlist")
		}
		return cobra.ExactArgs(1)(cmd, args)
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(wishlistFiles) > 0 {
			return expandWishlists(wishlistFiles, outputFile)
		}
		return expandSingleImage(args[0])
	},
}

// expandWishlists collects the fully-expanded image sets from all wishlist
// files, then either writes them as YAML to outputFile (when non-empty) or
// prints them to stdout one per line.
func expandWishlists(paths []string, outputFile string) error {
	var allImages []*lib.Image
	var expandErrors []string

	for _, path := range paths {
		images, errs, err := collectWishlistImages(path)
		if err != nil {
			return err
		}
		allImages = append(allImages, images...)
		expandErrors = append(expandErrors, errs...)
	}

	if outputFile != "" {
		if err := writeExpandedYAML(outputFile, allImages); err != nil {
			return err
		}
	} else {
		for _, img := range allImages {
			fmt.Println(img.WholeName())
		}
	}

	if len(expandErrors) > 0 {
		return fmt.Errorf("%d wish(es) failed to expand", len(expandErrors))
	}
	return nil
}

// collectWishlistImages reads and expands a single wishlist file.  It returns
// the concrete images, a list of non-fatal per-wish error messages, and a fatal
// error if the file cannot be read or parsed at all.
func collectWishlistImages(path string) (images []*lib.Image, expandErrors []string, err error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot read wishlist file %q: %w", path, err)
	}

	recipe, err := lib.ParseYamlRecipeV1(data)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot parse wishlist file %q: %w", path, err)
	}

	for wish := range recipe.Wishes {
		if len(wish.ExpandedTagImagesLayer) == 0 {
			msg := fmt.Sprintf("wish %q in %q expands to zero tags", wish.InputName, path)
			l.Log().WithFields(log.Fields{
				"input image": wish.InputName,
				"file":        path,
			}).Warning(msg)
			expandErrors = append(expandErrors, msg)
			continue
		}
		images = append(images, wish.ExpandedTagImagesLayer...)
	}
	return images, expandErrors, nil
}

// writeExpandedYAML writes the given images to path in the "images:" YAML
// format consumed by prune-images --expanded-images.
func writeExpandedYAML(path string, images []*lib.Image) error {
	type expandedList struct {
		Images []string `yaml:"images"`
	}
	list := expandedList{}
	for _, img := range images {
		list.Images = append(list.Images, img.WholeName())
	}

	data, err := yaml.Marshal(list)
	if err != nil {
		return fmt.Errorf("failed to marshal expanded image list: %w", err)
	}
	if err := ioutil.WriteFile(path, data, 0644); err != nil { // #nosec G306
		return fmt.Errorf("failed to write expanded image list to %q: %w", path, err)
	}

	l.Log().WithFields(log.Fields{
		"file":  path,
		"count": len(images),
	}).Info("Wrote expanded image list")
	return nil
}

// expandSingleImage handles the positional-argument mode: for a wildcard tag
// all matching registry tags are printed; for a concrete tag the manifest is
// fetched to confirm the image exists.
func expandSingleImage(imageRef string) error {
	img, err := lib.ParseImage(imageRef)
	if err != nil {
		return err
	}

	if !img.TagWildcard {
		_, err := img.GetManifestList()
		if err != nil {
			l.LogE(err).Fatal("No manifest exists for this tag")
			return err
		}
		return nil
	}

	r1, r2, err := img.ExpandWildcard()
	if err != nil {
		l.LogE(err).WithFields(log.Fields{"input image": img.WholeName()}).
			Error("Error in retrieving all the tags from the image")
		return err
	}

	var expandedTagImagesLayer, expandedTagImagesFlat []*lib.Image
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for img := range r1 {
			expandedTagImagesLayer = append(expandedTagImagesLayer, img)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for img := range r2 {
			expandedTagImagesFlat = append(expandedTagImagesFlat, img)
		}
	}()
	wg.Wait()

	_ = expandedTagImagesFlat // collected for completeness; layer images are the canonical output

	if len(expandedTagImagesLayer) == 0 {
		err = fmt.Errorf("wildcard expands to zero tags")
		l.LogE(err).WithFields(log.Fields{"input image": img.WholeName()}).
			Error("Wildcard expands to zero tags.")
		return err
	}
	for _, i := range expandedTagImagesLayer {
		fmt.Println(i.WholeName())
	}
	return nil
}
