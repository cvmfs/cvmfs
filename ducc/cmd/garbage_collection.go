package cmd

import (
	"fmt"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/cvmfs/ducc/lib"
)

func init() {
	rootCmd.AddCommand(garbageCollectionCmd)
}

var garbageCollectionCmd = &cobra.Command{
	Use:     "garbage-collection",
	Short:   "Removes layers that are not necessary anymore",
	Aliases: []string{"gc"},
	Args:    cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {

		// tried already to make them in parallel, we don't gain much
		// from ~1min to ~30 sec
		imagesUsed, _ := lib.FindAllUsedFlatImages("unpacked.cern.ch")
		imagesAll, _ := lib.FindAllFlatImages("unpacked.cern.ch")
		layersUsed, _ := lib.FindAllUsedLayers("unpacked.cern.ch")
		layersAll, _ := lib.FindAllLayers("unpacked.cern.ch")
		/*
			toPrintUsed, _ := json.MarshalIndent(imagesUsed, "", "  ")
			fmt.Print(string(toPrintUsed))
			imagesAll, _ := lib.FindAllUsedLayers("unpacked.cern.ch")
			toPrintAll, _ := json.MarshalIndent(imagesAll, "", "  ")
			fmt.Print(string(toPrintAll))

			//fmt.Printf("imagesUsed: %d\nimagesAll: %d", len(imagesUsed), len(imagesAll))
		*/
		fmt.Printf("\nimagesUsed: %d\nimagesAll: %d\nlayersUsed: %d\nlayersAll: %d\n", len(imagesUsed), len(imagesAll), len(layersUsed), len(layersAll))
		os.Exit(1)

		fmt.Println("Start")
		repo := args[0]
		llog := func(l *log.Entry) *log.Entry {
			return l.WithFields(log.Fields{"action": "garbage collect",
				"repo": repo,
			})
		}

		manifestToRemove, err := lib.FindImageToGarbageCollect(repo)
		if err != nil {
			llog(lib.LogE(err)).Warning(
				"Error in finding the image to remove from the scheduler, goin on...")
		}
		images2layers := make(map[string][]string)

		for _, manifest := range manifestToRemove {
			digest := strings.Split(manifest.Config.Digest, ":")[1]
			for _, layerStruct := range manifest.Layers {
				layerName := strings.Split(layerStruct.Digest, ":")[1]
				images2layers[digest] = append(images2layers[digest], layerName)
			}
		}

		for image, layers := range images2layers {
			for _, layer := range layers {
				err = lib.GarbageCollectSingleLayer(repo, image, layer)
				if err != nil {
					llog(lib.LogE(err)).Warning(
						"Error in removing a single layer from the repository, going on...")
				}
			}
		}

		os.Exit(0)
	},
}
