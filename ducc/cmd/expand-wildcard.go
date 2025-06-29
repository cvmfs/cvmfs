package cmd

import (
	"fmt"

	"sync"

	"github.com/cvmfs/ducc/lib"
	l "github.com/cvmfs/ducc/log"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(expandWildcardCmd)
}

var expandWildcardCmd = &cobra.Command{
	Use:   "expand-wildcard",
	Short: "List all the tags currently accessible under the image string",
	Args:  cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) (err error) {
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
