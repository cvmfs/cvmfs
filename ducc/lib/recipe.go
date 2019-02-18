package lib

import (
	"strings"

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
	Wishes []WishFriendly
}

func ParseYamlRecipeV1(data []byte) (Recipe, error) {
	recipeYamlV1 := YamlRecipeV1{}
	err := yaml.Unmarshal(data, &recipeYamlV1)
	if err != nil {
		return Recipe{}, err
	}
	recipe := Recipe{}
	for _, inputImage := range recipeYamlV1.Input {
		input, err := ParseImage(inputImage)
		if err != nil {
			LogE(err).WithFields(log.Fields{"image": inputImage}).Warning("Impossible to parse the image")
			continue
		}
		output := formatOutputImage(recipeYamlV1.OutputFormat, input)
		wish, err := CreateWish(inputImage, output, recipeYamlV1.CVMFSRepo, "", recipeYamlV1.User)
		if err != nil {
			LogE(err).Warning("Error in creating the wish")
			continue
		} else {
			recipe.Wishes = append(recipe.Wishes, wish)
		}
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
