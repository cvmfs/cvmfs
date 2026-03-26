package cmd

import (
	"fmt"
	"io/ioutil"
	"sync"

	"github.com/cvmfs/ducc/lib"
	l "github.com/cvmfs/ducc/log"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var wishlistFile string

func init() {
	expandWildcardCmd.Flags().StringVarP(&wishlistFile, "wishlist", "w", "", "path to a wishlist .yaml file; expands all images in the file and prints the full list")
	rootCmd.AddCommand(expandWildcardCmd)
}

var expandWildcardCmd = &cobra.Command{
	Use:   "expand-wildcard [image]",
	Short: "List all the tags currently accessible under the image string",
	// Require exactly one positional arg unless --wishlist is given.
	Args: func(cmd *cobra.Command, args []string) error {
		if cmd.Flags().Changed("wishlist") {
			return cobra.NoArgs(cmd, args)
		}
		return cobra.ExactArgs(1)(cmd, args)
	},
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		// --- wishlist mode ---
		if wishlistFile != "" {
			return expandWishlist(wishlistFile)
		}

		// --- single image mode (original behaviour) ---
		img, err := lib.ParseImage(args[0])
		if img.TagWildcard {
			r1, r2, errEx := img.ExpandWildcard()
			if errEx != nil {
				err = errEx
				l.LogE(err).WithFields(log.Fields{
					"input image": img.WholeName()}).
					Error("Error in retrieving all the tags from the image")
				return
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
			if len(expandedTagImagesLayer) == 0 {
				err = fmt.Errorf("wildcard expands to zero tags")
				l.LogE(err).WithFields(log.Fields{
					"input image": img.WholeName()}).
					Error("Wildcard expands to zero tags.")
				return
			}
			for _, i := range expandedTagImagesLayer {
				fmt.Println(i.WholeName())
			}
		} else {
			_, err := img.GetManifestList()
			if err != nil {
				l.LogE(err).Fatal("No manifest exists for this tag")
				return err
			}
		}

		return nil
	},
}

// expandWishlist reads a wishlist YAML file, expands all wildcard images, and
// prints the complete flat list of resolved image names (one per line).
func expandWishlist(path string) error {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		l.LogE(err).WithFields(log.Fields{"file": path}).Error("Cannot read wishlist file")
		return err
	}

	recipe, err := lib.ParseYamlRecipeV1(data)
	if err != nil {
		l.LogE(err).WithFields(log.Fields{"file": path}).Error("Cannot parse wishlist file")
		return err
	}

	var expandErrors []string
	for wish := range recipe.Wishes {
		if len(wish.ExpandedTagImagesLayer) == 0 {
			msg := fmt.Sprintf("wish %q expands to zero tags", wish.InputName)
			l.Log().WithFields(log.Fields{"input image": wish.InputName}).Warning(msg)
			expandErrors = append(expandErrors, msg)
			continue
		}
		for _, img := range wish.ExpandedTagImagesLayer {
			fmt.Println(img.WholeName())
		}
	}

	if len(expandErrors) > 0 {
		return fmt.Errorf("%d wish(es) failed to expand", len(expandErrors))
	}
	return nil
}
