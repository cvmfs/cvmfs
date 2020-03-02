package cmd

import (
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/cvmfs/ducc/lib"
)

var (
	cvmfsRepo string
)

func init() {
	convertSingularityImageCmd.Flags().StringVarP(&cvmfsRepo, "cvmfs-repo", "r", "", "Destination CVMFS repository")
	convertSingularityImageCmd.MarkFlagRequired("cvmfs-repo")
	rootCmd.AddCommand(convertSingularityImageCmd)
}

var convertSingularityImageCmd = &cobra.Command{
	Use:   "convert-singularity-image",
	Short: "Convert a single image to singularity",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		AliveMessage()

		if !lib.RepositoryExists(cvmfsRepo) {
			lib.Log().Error("The repository does not seems to exists.")
			os.Exit(RepoNotExistsError)
		}

		wish := lib.WishFriendly{Id: 0, CvmfsRepo: cvmfsRepo, InputName: args[0]}
		fields := log.Fields{"input image": wish.InputName,
			"repository": wish.CvmfsRepo}
		err := lib.ConvertWishSingularity(wish)
		if err != nil {
			lib.LogE(err).WithFields(fields).Error("Error in converting singularity image")
		}
	},
}
