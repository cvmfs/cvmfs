package daemon

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/cvmfs/ducc/db"
	"github.com/opencontainers/go-digest"
	"gopkg.in/yaml.v2"
)

type RecipeV1 struct {
	Wishlist     string
	CvmfsRepo    string
	User         string // TODO: Deprecate?
	OutputFormat string // TODO: Deprecate?
	Wishes       []db.WishIdentifier
}

type YamlRecipeV1 struct {
	Version      int      `yaml:"version"`
	User         string   `yaml:"user"`
	CVMFSRepo    string   `yaml:"cvmfs_repo"`
	OutputFormat string   `yaml:"output_format"`
	Input        []string `yaml:"input"`
}

// ParseYamlRecipeV1 parses a recipe in YAML format, and returns a RecipeV1 struct.
func ParseYamlRecipeV1(data []byte, wishlist string) (RecipeV1, error) {
	recipeYamlV1 := YamlRecipeV1{}
	err := yaml.Unmarshal(data, &recipeYamlV1)
	if err != nil {
		return RecipeV1{}, err
	}

	//TODO: Parse the cvmfs repo.
	// We could check that it exists, but it's not really necessary. We will have to check
	// later in case of deletion anyway.
	// However, we should check that it's a valid cvmfs repo name, to avoid problems or path traversal.

	recipe := RecipeV1{
		Wishlist:     wishlist,
		CvmfsRepo:    recipeYamlV1.CVMFSRepo,
		User:         recipeYamlV1.User,
		OutputFormat: recipeYamlV1.OutputFormat,
		Wishes:       make([]db.WishIdentifier, 0, len(recipeYamlV1.Input)),
	}

	for _, inputImage := range recipeYamlV1.Input {
		url, err := ParseImageURL(inputImage)
		if err != nil {
			return RecipeV1{}, err
		}
		identifier := db.WishIdentifier{
			Wishlist:              recipe.Wishlist,
			CvmfsRepository:       recipe.CvmfsRepo,
			InputTag:              url.Tag,
			InputTagWildcard:      url.TagWildcard,
			InputDigest:           url.Digest,
			InputRepository:       url.Repository,
			InputRegistryScheme:   url.Scheme,
			InputRegistryHostname: url.Registry,
		}
		recipe.Wishes = append(recipe.Wishes, identifier)
	}
	return recipe, nil
}

type ParsedWishInputURL struct {
	Scheme      string
	Registry    string
	Repository  string
	Tag         string
	TagWildcard bool
	Digest      digest.Digest
}

func ParseImageURL(imageUrl string) (ParsedWishInputURL, error) {
	// TODO: This is copied from lib/parse and should be verified
	url, err := url.Parse(imageUrl)
	if err != nil {
		return ParsedWishInputURL{}, err
	}
	if url.Host == "" {
		// TODO: Do we really want to support this?
		// likely the protocol `https://` is missing in the image string.
		// worth to try to append it, and re-parse the image
		image2 := "https://" + imageUrl
		img2, err2 := ParseImageURL(image2)
		if err2 == nil {
			return img2, err2
		}

		// some other error, let's return the first error
		return ParsedWishInputURL{}, fmt.Errorf("Impossible to identify the registry of the image: %s", imageUrl)
	}
	if url.Scheme != "http" && url.Scheme != "https" {
		return ParsedWishInputURL{}, fmt.Errorf("Unsupported protocol: %s", url.Scheme)
	}

	if url.Path == "" {
		return ParsedWishInputURL{}, fmt.Errorf("Impossible to identify the repository of the image: %s", imageUrl)
	}
	colonPathSplitted := strings.Split(url.Path, ":")
	if len(colonPathSplitted) == 0 {
		return ParsedWishInputURL{}, fmt.Errorf("Impossible to identify the path of the image: %s", imageUrl)
	}
	// no split happened, hence we don't have neither a tag nor a digest, but only a path
	if len(colonPathSplitted) == 1 {

		// we remove the first  and the trailing `/`
		repository := strings.TrimLeft(colonPathSplitted[0], "/")
		repository = strings.TrimRight(repository, "/")
		if repository == "" {
			return ParsedWishInputURL{}, fmt.Errorf("Impossible to find the repository for: %s", imageUrl)
		}
		return ParsedWishInputURL{
			Scheme:     url.Scheme,
			Registry:   url.Host,
			Repository: repository,
			Tag:        "latest",
		}, nil

	}
	if len(colonPathSplitted) > 3 {
		fmt.Println(colonPathSplitted)
		return ParsedWishInputURL{}, fmt.Errorf("Impossible to parse the string into an image, too many `:` in : %s", imageUrl)
	}
	// the colon `:` is used also as separator in the digest between sha256
	// and the actuall digest, a len(pathSplitted) == 2 could either means
	// a repository and a tag or a repository and an hash, in the case of
	// the hash however the split will be more complex.  Now we split for
	// the at `@` which separate the digest from everything else. If this
	// split produce only one result we have a repository and maybe a tag,
	// if it produce two we have a repository, maybe a tag and definitely a
	// digest, if it produce more than two we have an error.
	atPathSplitted := strings.Split(url.Path, "@")
	if len(atPathSplitted) > 2 {
		return ParsedWishInputURL{}, fmt.Errorf("To many `@` in the image name: %s", imageUrl)
	}
	var repoTag string
	var parsedDigest digest.Digest
	if len(atPathSplitted) == 2 {
		parsedDigest, err = digest.Parse(atPathSplitted[1])
		if err != nil {
			return ParsedWishInputURL{}, fmt.Errorf("Impossible to parse the digest: %s", atPathSplitted[1])
		}
		repoTag = atPathSplitted[0]
	}
	if len(atPathSplitted) == 1 {
		repoTag = atPathSplitted[0]
	}
	// finally we break up also the repoTag to find out if we have also a
	// tag or just a repository name
	colonRepoTagSplitted := strings.Split(repoTag, ":")

	// only the repository, without the tag
	if len(colonRepoTagSplitted) == 1 {
		repository := strings.TrimLeft(colonRepoTagSplitted[0], "/")
		repository = strings.TrimRight(repository, "/")
		if repository == "" {
			return ParsedWishInputURL{}, fmt.Errorf("Impossible to find the repository for: %s", imageUrl)
		}
		tag := ""
		if parsedDigest == "" {
			// TODO: Warn the user that we are defauting to "latest"
			tag = "latest"
		}
		return ParsedWishInputURL{
			Scheme:     url.Scheme,
			Registry:   url.Host,
			Repository: repository,
			Digest:     parsedDigest,
			Tag:        tag,
		}, nil
	}

	// both repository and tag
	if len(colonRepoTagSplitted) == 2 {
		repository := strings.TrimLeft(colonRepoTagSplitted[0], "/")
		repository = strings.TrimRight(repository, "/")
		if repository == "" {
			return ParsedWishInputURL{}, fmt.Errorf("Impossible to find the repository for: %s", imageUrl)
		}
		tag := colonRepoTagSplitted[1]
		if parsedDigest != "" {
			// Digest takes precedence over the tag
			tag = ""
		}
		tagWildcard := strings.Contains(tag, "*")

		return ParsedWishInputURL{
			Scheme:      url.Scheme,
			Registry:    url.Host,
			Repository:  repository,
			Digest:      parsedDigest,
			Tag:         tag,
			TagWildcard: tagWildcard,
		}, nil
	}
	return ParsedWishInputURL{}, fmt.Errorf("Impossible to parse the wish: %s", imageUrl)
}

func parseRecipe(recipe string) (RecipeV1, error) {
	return RecipeV1{}, errors.New("not implemented")
}
