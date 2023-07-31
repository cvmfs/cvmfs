package db

import (
	"database/sql"

	"gopkg.in/yaml.v2"
)

type RecipeV1 struct {
	Source       string
	CvmfsRepo    string
	User         string // TODO: Deprecate?
	OutputFormat string // TODO: Deprecate?
	Wishes       []WishIdentifier
}

// ImportRecipeV1 imports a recipe into the database.
// It returns a slice of wish IDs that were created.
func ImportRecipeV1(recipe RecipeV1) (created []Wish, deleted []Wish, err error) {
	if err != nil {
		return nil, nil, err
	}

	tx, err := GetTransaction()
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()

	// We create the new wishes that are in the database
	newIdentifiers, err := filterNewWishes(tx, recipe.Wishes)
	if err != nil {
		return nil, nil, err
	}
	newWishes, err := CreateWishesByIdentifiers(tx, newIdentifiers)
	if err != nil {
		return nil, nil, err
	}
	// Find wishes with the same source that are not in the recipe
	// They should be deleted from the database
	toDelete, err := getDeletedWishIDs(tx, recipe.Source, recipe.Wishes)
	if err != nil {
		return nil, nil, err
	}
	toDeleteIDs := make([]WishID, 0, len(toDelete))
	for _, wish := range toDelete {
		toDeleteIDs = append(toDeleteIDs, wish.ID)
	}
	err = DeleteWishesByIDs(tx, toDeleteIDs)
	if err != nil {
		return nil, nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, nil, err
	}

	return newWishes, toDelete, nil
}

// filterNewWishes returns a slice of identifiers that are not already in the database
func filterNewWishes(tx *sql.Tx, identifiers []WishIdentifier) ([]WishIdentifier, error) {
	existingWishes, err := GetWishesByValues(tx, identifiers)
	if err != nil {
		return nil, err
	}
	numNewWishes := len(identifiers) - len(existingWishes)
	if numNewWishes == 0 {
		return []WishIdentifier{}, nil
	}

	// We create a map of existing wishes for fast lookup
	existingWishesMap := make(map[WishIdentifier]struct{}, len(existingWishes))
	for _, wish := range existingWishes {
		existingWishesMap[wish.Identifier] = struct{}{}
	}

	out := make([]WishIdentifier, 0, numNewWishes)
	for _, identifier := range identifiers {
		_, exists := existingWishesMap[identifier]
		if !exists {
			out = append(out, identifier)
		}
	}
	return out, nil
}

// getDeletedWishes returns a slice Wishes that have the same source as the recipe, but are not in the recipe
// This is used to delete wishes that are not in the recipe anymore
func getDeletedWishIDs(tx *sql.Tx, source string, identifiers []WishIdentifier) ([]Wish, error) {

	wishesInDB, err := GetWishesBySource(tx, source)
	if err != nil {
		return nil, err
	}
	numDeletedWishes := len(wishesInDB) - len(identifiers)
	if numDeletedWishes == 0 {
		return []Wish{}, nil
	}

	// We create a map of wishes in the recipe for fast lookup
	identifiersMap := make(map[WishIdentifier]struct{}, len(identifiers))
	for _, identifier := range identifiers {
		identifiersMap[identifier] = struct{}{}
	}

	out := make([]Wish, 0, numDeletedWishes)
	for _, wish := range wishesInDB {
		_, exists := identifiersMap[wish.Identifier]
		if !exists {
			out = append(out, wish)
		}
	}
	return out, nil
}

type YamlRecipeV1 struct {
	Version      int      `yaml:"version"`
	User         string   `yaml:"user"`
	CVMFSRepo    string   `yaml:"cvmfs_repo"`
	OutputFormat string   `yaml:"output_format"`
	Input        []string `yaml:"input"`
}

// ParseYamlRecipeV1 parses a recipe in YAML format, and returns a RecipeV1 struct.
func ParseYamlRecipeV1(data []byte, source string) (RecipeV1, error) {
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
		Source:       source,
		CvmfsRepo:    recipeYamlV1.CVMFSRepo,
		User:         recipeYamlV1.User,
		OutputFormat: recipeYamlV1.OutputFormat,
		Wishes:       make([]WishIdentifier, 0, len(recipeYamlV1.Input)),
	}

	for _, inputImage := range recipeYamlV1.Input {
		url, err := ParseWishInputURL(inputImage)
		if err != nil {
			return RecipeV1{}, err
		}
		identifier := WishIdentifier{
			Source:                recipe.Source,
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
