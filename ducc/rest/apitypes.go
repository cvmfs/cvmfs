package rest

import (
	"fmt"

	"github.com/cvmfs/ducc/db"
	"github.com/google/uuid"
)

type Wish struct {
	Id                    uuid.TaskID `json:"id"`
	CvmfsRepository       string      `json:"cvmfsRepository"`
	InputRegistryScheme   string      `json:"inputRegistryScheme"`
	InputRegistryHostname string      `json:"inputRegistryHostname"`
	InputRepository       string      `json:"inputRepository"`
	InputDigest           string      `json:"inputDigest"`
	InputTag              string      `json:"inputTag"`
	InputTagWildcard      bool        `json:"inputTagWildcard"`
	OutputURL             string      `json:"outputURL"`
	Source                string      `json:"source"`

	CreateLayers    db.ValueWithDefault[bool] `json:"createLayers"`
	CreateThinImage db.ValueWithDefault[bool] `json:"createThinImage"`
	CreatePodman    db.ValueWithDefault[bool] `json:"createPodman"`
	CreateFlat      db.ValueWithDefault[bool] `json:"createFlat"`

	WebhookEnabled      db.ValueWithDefault[bool] `json:"webhookEnabled"`
	FullSyncIntervalSec int64                     `json:"fullSyncIntervalSec"`

	LastConfigUpdate string `json:"lastConfigUpdate"`
	LastFullSync     string `json:"lastFullSync"`

	SyncedImages []ImageSummary `json:"syncedImages"`
}

type Image struct {
	Id             uuid.TaskID `json:"id"`
	URL            string      `json:"url"`
	RegistryScheme string      `json:"registryScheme"`
	RegistryHost   string      `json:"registryHost"`
	Repository     string      `json:"repository"`
	Tag            string      `json:"tag"`
	Digest         string      `json:"digest"`

	Wishes []WishSummary `json:"wishes"`
}

func ToImageSummary(img Image) ImageSummary {
	return ImageSummary{
		Id:  img.Id,
		URL: img.URL,
	}
}

type ImageSummary struct {
	Id  uuid.TaskID `json:"id"`
	URL string      `json:"url"`
}

type WishSummary struct {
	Id  uuid.TaskID `json:"id"`
	URL string      `json:"url"`
}

func DbWishToWishSummary(w db.Wish) WishSummary {
	digestOrTag := ""
	if w.Identifier.InputDigest != "" {
		digestOrTag = "@" + w.Identifier.InputDigest.String()
	} else if w.Identifier.InputTag != "" {
		digestOrTag = ":" + w.Identifier.InputTag
	}

	return WishSummary{
		Id:  w.ID,
		URL: fmt.Sprintf("%s://%s/%s%s", w.Identifier.InputRegistryScheme, w.Identifier.InputRegistryHostname, w.Identifier.InputRepository, digestOrTag),
	}
}

func CreateApiWishFromDBWish(wish db.Wish, images []db.Image) Wish {
	imageSummaries := make([]ImageSummary, len(images))
	for i, image := range images {
		imageSummaries[i] = ImageSummary{Id: image.ID, URL: image.AsURL()}
	}

	return Wish{
		Id:                    wish.ID,
		CvmfsRepository:       wish.Identifier.CvmfsRepository,
		InputRegistryScheme:   wish.Identifier.InputRegistryScheme,
		InputRegistryHostname: wish.Identifier.InputRegistryHostname,
		InputRepository:       wish.Identifier.InputRepository,
		InputDigest:           wish.Identifier.InputDigest.String(),
		InputTag:              wish.Identifier.InputTag,
		InputTagWildcard:      wish.Identifier.InputTagWildcard,
		OutputURL:             "", //TODO
		Source:                wish.Identifier.Source,

		CreateLayers:    wish.OutputOptions.CreateLayers,
		CreateThinImage: wish.OutputOptions.CreateThinImage,
		CreatePodman:    wish.OutputOptions.CreatePodman,
		CreateFlat:      wish.OutputOptions.CreateFlat,

		WebhookEnabled:      wish.ScheduleOptions.WebhookEnabled,
		FullSyncIntervalSec: 10, //TODO

		LastConfigUpdate: "", //TODO
		LastFullSync:     "", //TODO

		SyncedImages: imageSummaries,
	}
}

func CreateApiImageFromDBImage(image db.Image, wishes []db.Wish) Image {
	wishSummaries := make([]WishSummary, len(wishes))
	for j, wish := range wishes {
		wishSummaries[j] = DbWishToWishSummary(wish)
	}

	return Image{
		Id:             image.ID,
		URL:            image.AsURL(),
		RegistryScheme: image.RegistryScheme,
		RegistryHost:   image.RegistryHost,
		Repository:     image.Repository,
		Tag:            image.Tag,
		Digest:         image.Digest.String(),
		Wishes:         wishSummaries,
	}

}
