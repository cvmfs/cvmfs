package updater

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/cvmfs/ducc/db"
	"github.com/cvmfs/ducc/registry"
)

func ExpandWildcardAndStoreImages(wish db.Wish) (created []db.Image, updated []db.Image, deleted []db.Image, err error) {
	images, err := ExpandWildcard(wish)
	if err != nil {
		return nil, nil, nil, err
	}

	tx, err := db.GetTransaction()
	if err != nil {
		return nil, nil, nil, err
	}
	defer tx.Rollback()

	// Ensure that the wish still exists
	if _, err := db.GetWishByID(tx, wish.ID); err != nil {
		return nil, nil, nil, err
	}

	// Update the images for the wish
	// TODO: Use in a suitable way the returned values
	created, updated, deleted, err = db.UpdateImagesForWish(tx, wish.ID, images)
	if err != nil {
		return nil, nil, nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, nil, nil, err
	}

	return created, updated, deleted, nil
}

func ExpandWildcard(wish db.Wish) ([]db.Image, error) {
	if !wish.Identifier.InputTagWildcard {
		// Instead of returning an error, we return the original db.Wish.
		// This is for convenience, so the caller doesn't need to know if it contains a wildcard or not.
		return []db.Image{{
			RegistryScheme: wish.Identifier.InputRegistryScheme,
			RegistryHost:   wish.Identifier.InputRegistryHostname,
			Repository:     wish.Identifier.InputRepository,
			Tag:            wish.Identifier.InputTag,
			Digest:         wish.Identifier.InputDigest,
		}}, nil
	}

	// We do the request through the registry library, so auth and throttling are handled automatically.
	registry := registry.GetOrCreateRegistry(registry.ContainerRegistryIdentifier{Scheme: wish.Identifier.InputRegistryScheme, Hostname: wish.Identifier.InputRegistryHostname})
	tags, err := fetchTags(registry, wish)
	if err != nil {
		return nil, err
	}

	filteredTags, err := filterUsingGlob(wish.Identifier.InputTag, tags)
	if err != nil {
		return nil, err
	}

	images := make([]db.Image, 0, len(filteredTags))
	for _, tag := range filteredTags {
		images = append(images, db.Image{
			RegistryScheme: wish.Identifier.InputRegistryScheme,
			RegistryHost:   wish.Identifier.InputRegistryHostname,
			Repository:     wish.Identifier.InputRepository,
			Tag:            tag,
			Digest:         "",
		})
	}

	return images, nil
}

func filterUsingGlob(pattern string, toFilter []string) ([]string, error) {
	result := make([]string, 0)
	regexPattern := strings.ReplaceAll(pattern, "*", ".*")
	regex, err := regexp.Compile(regexPattern)
	if err != nil {
		return result, err
	}
	regex.Longest()
	for _, toCheck := range toFilter {
		s := regex.FindString(toCheck)
		if s == "" {
			continue
		}
		if s == toCheck {
			result = append(result, s)
		}
	}
	return result, nil
}

func fetchTags(registry *registry.ContainerRegistry, wish db.Wish) ([]string, error) {
	url := fmt.Sprintf("%s://%s/%s/tags/list", wish.Identifier.InputRegistryScheme, wish.Identifier.InputRegistryHostname, wish.Identifier.InputRepository)
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Add("Accept", "application/json")
	res, err := registry.PerformRequest(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var tagList struct{ Tags []string }
	if err = json.NewDecoder(res.Body).Decode(&tagList); err != nil {
		return nil, fmt.Errorf("error in decoding the tags from the server: %s", err)
	}
	return tagList.Tags, nil
}
