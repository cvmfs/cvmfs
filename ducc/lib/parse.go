package lib

import (
	"fmt"
	"net/url"
	"strings"
)

func ParseImage(image string) (img Image, err error) {
	url, err := url.Parse(image)
	if err != nil {
		return Image{}, err
	}
	if url.Host == "" {

		// likely the protocol `https://` is missing in the image string.
		// worth to try to append it, and re-parse the image
		image2 := "https://" + image
		img2, err2 := ParseImage(image2)
		if err2 == nil {
			return img2, err2
		}

		// some other error, let's return the first error
		return Image{}, fmt.Errorf("Impossible to identify the registry of the image: %s", image)
	}
	if url.Path == "" {
		return Image{}, fmt.Errorf("Impossible to identify the repository of the image: %s", image)
	}
	colonPathSplitted := strings.Split(url.Path, ":")
	if len(colonPathSplitted) == 0 {
		return Image{}, fmt.Errorf("Impossible to identify the path of the image: %s", image)
	}
	// no split happened, hence we don't have neither a tag nor a digest, but only a path
	if len(colonPathSplitted) == 1 {

		// we remove the first  and the trailing `/`
		repository := strings.TrimLeft(colonPathSplitted[0], "/")
		repository = strings.TrimRight(repository, "/")
		if repository == "" {
			return Image{}, fmt.Errorf("Impossible to find the repository for: %s", image)
		}
		return Image{
			Scheme:     url.Scheme,
			Registry:   url.Host,
			Repository: repository,
		}, nil

	}
	if len(colonPathSplitted) > 3 {
		fmt.Println(colonPathSplitted)
		return Image{}, fmt.Errorf("Impossible to parse the string into an image, too many `:` in : %s", image)
	}
	// the colon `:` is used also as separator in the digest between sha256
	// and the actual digest, a len(pathSplitted) == 2 could either mean
	// a repository and a tag or a repository and an hash, in the case of
	// the hash however the split will be more complex.  Now we split for
	// the at `@` which separate the digest from everything else. If this
	// split produces only one result we have a repository and maybe a tag,
	// if it produces two we have a repository, maybe a tag and definitely a
	// digest, if it produces more than two we have an error.
	atPathSplitted := strings.Split(url.Path, "@")
	if len(atPathSplitted) > 2 {
		return Image{}, fmt.Errorf("Too many `@` in the image name: %s", image)
	}
	var repoTag, digest string
	if len(atPathSplitted) == 2 {
		digest = atPathSplitted[1]
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
			return Image{}, fmt.Errorf("Impossible to find the repository for: %s", image)
		}
		return Image{
			Scheme:     url.Scheme,
			Registry:   url.Host,
			Repository: repository,
			Digest:     digest,
		}, nil
	}

	// both repository and tag
	if len(colonRepoTagSplitted) == 2 {
		repository := strings.TrimLeft(colonRepoTagSplitted[0], "/")
		repository = strings.TrimRight(repository, "/")
		if repository == "" {
			return Image{}, fmt.Errorf("Impossible to find the repository for: %s", image)
		}
		tag := colonRepoTagSplitted[1]
		return Image{
			Scheme:      url.Scheme,
			Registry:    url.Host,
			Repository:  repository,
			Tag:         tag,
			Digest:      digest,
			TagWildcard: strings.Contains(tag, `*`),
		}, nil
	}
	return Image{}, fmt.Errorf("Impossible to parse the image: %s", image)
}
