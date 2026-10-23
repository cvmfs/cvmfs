package lib

import (
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

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
	IgnoreErrors []string `yaml:"ignoreErrors"`
}

type IgnoreErrorsConfig struct {
	IgnoreErrors []string `yaml:"ignoreErrors"`
}

type Recipe struct {
	Repo             string
	Wishes           chan WishFriendly
	IgnoreErrorsList []string
	OutputFormat     string
}

// loadIgnoreErrorsList loads the ignore errors list from an external file if DUCC_IGNORE_ERRORS_FILE env var is set
func loadIgnoreErrorsList() ([]string, error) {
	ignoreFilePath := os.Getenv("DUCC_IGNORE_ERRORS_FILE")
	if ignoreFilePath == "" {
		return []string{}, nil
	}

	data, err := ioutil.ReadFile(ignoreFilePath)
	if err != nil {
		l.LogE(err).WithFields(log.Fields{"file": ignoreFilePath}).Warning("Failed to read ignore errors file from DUCC_IGNORE_ERRORS_FILE")
		return []string{}, err
	}

	var ignoreConfig IgnoreErrorsConfig
	err = yaml.Unmarshal(data, &ignoreConfig)
	if err != nil {
		l.LogE(err).WithFields(log.Fields{"file": ignoreFilePath}).Warning("Failed to parse ignore errors file")
		return []string{}, err
	}

	l.Log().WithFields(log.Fields{
		"file":  ignoreFilePath,
		"count": len(ignoreConfig.IgnoreErrors),
	}).Info("Loaded ignore errors list from external file")

	return ignoreConfig.IgnoreErrors, nil
}

func ParseYamlRecipeV1(data []byte) (Recipe, error) {
	recipeYamlV1 := YamlRecipeV1{}
	err := yaml.Unmarshal(data, &recipeYamlV1)
	recipe := Recipe{}
	recipe.Repo = recipeYamlV1.CVMFSRepo
	recipe.Wishes = make(chan WishFriendly, 500)
	recipe.OutputFormat = recipeYamlV1.OutputFormat

	// Load ignore errors list from recipe file
	recipe.IgnoreErrorsList = recipeYamlV1.IgnoreErrors

	// Load and merge ignore errors list from external file if DUCC_IGNORE_ERRORS_FILE is set
	externalIgnoreList, errIgnore := loadIgnoreErrorsList()
	if errIgnore == nil && len(externalIgnoreList) > 0 {
		recipe.IgnoreErrorsList = append(recipe.IgnoreErrorsList, externalIgnoreList...)
		l.Log().WithFields(log.Fields{
			"total_count": len(recipe.IgnoreErrorsList),
		}).Info("Merged ignore errors lists from recipe and external file")
	}

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

	registryMap := make(map[string][]Image)
	for _, inputImage := range recipeYamlV1.Input {
		input, err := ParseImage(inputImage)
		if err != nil {
			l.LogE(err).WithFields(log.Fields{"image": inputImage}).Warning("Impossible to parse the image")
		}
		registryMap[input.Registry] = append(registryMap[input.Registry], input)
	}
	for reg, inputImages := range registryMap {
		wg.Add(1)
		go func(inputImages []Image, reg string) {
			defer wg.Done()
			for _, input := range inputImages {
				if reg == "gitlab-registry.cern.ch" {
					time.Sleep(500*time.Millisecond + time.Duration(rand.Intn(500))*time.Millisecond)
				}

				output := formatOutputImage(recipeYamlV1.OutputFormat, input)
				wish, err := CreateWish(input, output, recipeYamlV1.CVMFSRepo, recipeYamlV1.User, recipeYamlV1.User)
				if err != nil {
					l.LogE(err).Warning("Error in creating the wish")
				} else {
					recipe.Wishes <- wish
				}
			}
		}(inputImages, reg)
	}
	return recipe, nil
}

func formatOutputImage(OutputFormat string, inputImage Image) string {

	if OutputFormat == "" {
		OutputFormat = "$(scheme)://$(registry)/$(repository)_thin:$(tag)"
	}

	s := strings.Replace(OutputFormat, "$(scheme)", inputImage.Scheme, 5)
	s = strings.Replace(s, "$(registry)", inputImage.Registry, 5)
	s = strings.Replace(s, "$(repository)", inputImage.Repository, 5)
	s = strings.Replace(s, "$(digest)", inputImage.Digest, 5)
	s = strings.Replace(s, "$(tag)", inputImage.Tag, 5)
	s = strings.Replace(s, "$(reference)", inputImage.GetReference(), 5)
	s = strings.Replace(s, "$(image)", inputImage.Repository+inputImage.GetReference(), 5)

	return s
}
