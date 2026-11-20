package lib

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	l "github.com/cvmfs/ducc/log"
	"github.com/cvmfs/ducc/sif"
	log "github.com/sirupsen/logrus"

	"gopkg.in/yaml.v2"
)

type YamlRecipeV1 struct {
	Version      int              `yaml:"version"`
	User         string           `yaml:"user"`
	CVMFSRepo    string           `yaml:"cvmfs_repo"`
	OutputFormat string           `yaml:"output_format"`
	Input        []string         `yaml:"input"`
	IgnoreErrors []string         `yaml:"ignoreErrors"`
	SIFInput     []SIFWish        `yaml:"sif_input"`
	SIFOptions   YamlSIFOptionsV1 `yaml:"sif_options"`
}

// YamlSIFOptionsV1 holds ingest tuning shared by all sif_input entries;
// unset fields keep the convert-sif defaults.
type YamlSIFOptionsV1 struct {
	ContiguousCatalogs bool   `yaml:"contiguous_catalogs"`
	CatalogMaxWeight   *int   `yaml:"catalog_max_weight"`
	CatalogMinWeight   *int   `yaml:"catalog_min_weight"`
	Workers            int    `yaml:"workers"`
	CacheSizeMiB       int    `yaml:"cache_size_mib"`
	PlainHTTP          bool   `yaml:"plain_http"`
	SpoolDir           string `yaml:"spool_dir"`
}

type IgnoreErrorsConfig struct {
	IgnoreErrors []string `yaml:"ignoreErrors"`
}

type Recipe struct {
	Repo             string
	Wishes           chan WishFriendly
	IgnoreErrorsList []string
	OutputFormat     string
	// NumInputs counts the docker inputs, so that callers can tell a
	// SIF-only recipe apart before draining Wishes.
	NumInputs  int
	SIFWishes  []SIFWish
	SIFOptions YamlSIFOptionsV1
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
	if err := validateSIFInput(recipeYamlV1.SIFInput); err != nil {
		return recipe, err
	}
	if err := recipeYamlV1.SIFOptions.validate(); err != nil {
		return recipe, err
	}
	recipe.SIFWishes = recipeYamlV1.SIFInput
	recipe.SIFOptions = recipeYamlV1.SIFOptions
	recipe.NumInputs = len(recipeYamlV1.Input)

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

// validateSIFInput rejects the whole recipe on a bad entry: publishing the
// others would hide the typo, and two entries with the same name would
// relink sif/<name> back and forth on every run.
func validateSIFInput(wishes []SIFWish) error {
	var errs []string
	seen := make(map[string]int)
	for i := range wishes {
		if wishes[i].Name == "" {
			wishes[i].Name = sif.DefaultName(wishes[i].Source)
		}
		w := wishes[i]
		where := fmt.Sprintf("sif_input[%d]", i)
		if w.Source == "" {
			errs = append(errs, where+": source is required")
		}
		if err := sif.ValidateName(w.Name); err != nil {
			errs = append(errs, fmt.Sprintf("%s (%s): %v", where, w.Source, err))
		} else if j, dup := seen[w.Name]; dup {
			errs = append(errs, fmt.Sprintf("%s: name %q already used by sif_input[%d]", where, w.Name, j))
		} else {
			seen[w.Name] = i
		}
		if w.Digest != "" {
			if err := sif.ValidateDigest(w.Digest); err != nil {
				errs = append(errs, fmt.Sprintf("%s (%s): %v", where, w.Source, err))
			}
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("invalid sif_input:\n  %s", strings.Join(errs, "\n  "))
	}
	return nil
}

func (o YamlSIFOptionsV1) validate() error {
	for name, v := range map[string]*int{"catalog_max_weight": o.CatalogMaxWeight, "catalog_min_weight": o.CatalogMinWeight} {
		if v != nil && *v < 0 {
			return fmt.Errorf("sif_options.%s must not be negative", name)
		}
	}
	if o.Workers < 0 || o.CacheSizeMiB < 0 {
		return fmt.Errorf("sif_options.workers and sif_options.cache_size_mib must not be negative")
	}
	return nil
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
