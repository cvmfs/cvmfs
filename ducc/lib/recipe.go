package lib

import (
	"strings"
	"sync"

	l "github.com/cvmfs/ducc/log"
	log "github.com/sirupsen/logrus"

	"gopkg.in/yaml.v2"
)

type YamlRecipeV1 struct {
	Version      int      `yaml:"version"`
	User         string   `yaml:"user"`
	CVMFSRepo    string   `yaml:"cvmfs_repo"`
	OutputFormat string   `yaml:"output_format"`
	Input        []string `yaml:"input"`
}

type Recipe struct {
	Repo   string
	Wishes chan WishFriendly
}

func ParseYamlRecipeV1(data []byte) (Recipe, error) {
	recipeYamlV1 := YamlRecipeV1{}
	err := yaml.Unmarshal(data, &recipeYamlV1)
	recipe := Recipe{}
	recipe.Repo = recipeYamlV1.CVMFSRepo
	recipe.Wishes = make(chan WishFriendly, 500)
	var wg sync.WaitGroup
	defer func() {
		go func() {
			wg.Wait()
			close(recipe.Wishes)
		}()
	}()
	if err != nil {
		return recipe, err
	}
	for _, inputImage := range recipeYamlV1.Input {
		wg.Add(1)
		go func(inputImage string) {
			defer wg.Done()
			input, err := ParseImage(inputImage)
			if err != nil {
				l.LogE(err).WithFields(log.Fields{"image": inputImage}).Warning("Impossible to parse the image")
				return
			}
			output := formatOutputImage(recipeYamlV1.OutputFormat, input)
			wish, err := CreateWish(inputImage, output, recipeYamlV1.CVMFSRepo, recipeYamlV1.User, recipeYamlV1.User)
			if err != nil {
				l.LogE(err).Warning("Error in creating the wish")
			} else {
				recipe.Wishes <- wish
			}
		}(inputImage)
	}
	return recipe, nil
}

func formatOutputImage(OutputFormat string, inputImage Image) string {

	s := strings.Replace(OutputFormat, "$(scheme)", inputImage.Scheme, 5)
	s = strings.Replace(s, "$(registry)", inputImage.Registry, 5)
	s = strings.Replace(s, "$(repository)", inputImage.Repository, 5)
	s = strings.Replace(s, "$(digest)", inputImage.Digest, 5)
	s = strings.Replace(s, "$(tag)", inputImage.Tag, 5)
	s = strings.Replace(s, "$(reference)", inputImage.GetReference(), 5)
	s = strings.Replace(s, "$(image)", inputImage.Repository+inputImage.GetReference(), 5)

	return s
}
