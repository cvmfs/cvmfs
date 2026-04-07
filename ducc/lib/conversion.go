package lib

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	constants "github.com/cvmfs/ducc/constants"
	cvmfs "github.com/cvmfs/ducc/cvmfs"
	da "github.com/cvmfs/ducc/docker-api"
	l "github.com/cvmfs/ducc/log"
	notification "github.com/cvmfs/ducc/notification"
	singularity "github.com/cvmfs/ducc/singularity"
	temp "github.com/cvmfs/ducc/temp"

	dockerImage "github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	log "github.com/sirupsen/logrus"
)

var NoPasswordError = 101

type ConversionResult int

const (
	ConversionNotFound = iota
	ConversionMatch    = iota
	ConversionNotMatch = iota
)

const ConversionUnknown ConversionResult = -1

type ConversionSummary struct {
	Added            []string
	Updated          []string
	AlreadyConverted []string
}

func removeImage(images []string, image string) []string {
	filtered := images[:0]
	for _, existing := range images {
		if existing != image {
			filtered = append(filtered, existing)
		}
	}
	return filtered
}

func containsImage(images []string, image string) bool {
	for _, existing := range images {
		if existing == image {
			return true
		}
	}
	return false
}

func (s *ConversionSummary) addUnique(dst *[]string, image string) {
	if image == "" {
		return
	}
	for _, existing := range *dst {
		if existing == image {
			return
		}
	}
	*dst = append(*dst, image)
}

func (s *ConversionSummary) Add(result ConversionResult, image string) {
	switch result {
	case ConversionNotFound:
		s.Updated = removeImage(s.Updated, image)
		s.AlreadyConverted = removeImage(s.AlreadyConverted, image)
		s.addUnique(&s.Added, image)
	case ConversionNotMatch:
		if containsImage(s.Added, image) {
			return
		}
		s.AlreadyConverted = removeImage(s.AlreadyConverted, image)
		s.addUnique(&s.Updated, image)
	case ConversionMatch:
		if containsImage(s.Added, image) || containsImage(s.Updated, image) {
			return
		}
		s.addUnique(&s.AlreadyConverted, image)
	}
}

func (s *ConversionSummary) Merge(other ConversionSummary) {
	for _, image := range other.Added {
		s.Add(ConversionNotFound, image)
	}
	for _, image := range other.Updated {
		s.Add(ConversionNotMatch, image)
	}
	for _, image := range other.AlreadyConverted {
		s.Add(ConversionMatch, image)
	}
}

func platformLabel(manifestEntry da.ManifestListItem) string {
	parts := []string{}
	if manifestEntry.Platform.OS != "" {
		parts = append(parts, manifestEntry.Platform.OS)
	}
	if manifestEntry.Platform.Architecture != "" {
		parts = append(parts, manifestEntry.Platform.Architecture)
	}
	if manifestEntry.Platform.Variant != nil && *manifestEntry.Platform.Variant != "" {
		parts = append(parts, *manifestEntry.Platform.Variant)
	}
	return strings.Join(parts, "/")
}

func imageNameWithPlatform(inputImage *Image, manifestEntry da.ManifestListItem) string {
	imageName := inputImage.GetSimpleName()
	platform := platformLabel(manifestEntry)
	if platform == "" {
		return imageName
	}
	return fmt.Sprintf("%s (%s)", imageName, platform)
}

func GetNameWithArch(manifestEntry da.ManifestListItem) (nameWithArch string) {
	if manifestEntry.Platform.Architecture == "" {
		return ""
	}
	if manifestEntry.Platform.Variant != nil {
		return filepath.Join(".multiarch", manifestEntry.Platform.Architecture+":"+*manifestEntry.Platform.Variant)
	}
	return filepath.Join(".multiarch", manifestEntry.Platform.Architecture)
}

// archAliases maps non-normalized Docker/OCI architecture identifiers to
// candidate .multiarch/ directory names in preference order.  The first
// candidate directory that exists in the repository is used as the symlink
// target.  Architectures with a variant use the "arch:variant" form that
// GetNameWithArch writes to disk (e.g. "arm:v6", not "arm/v6").
//
// "arm64" is listed as an alias for "arm64:v8" because the OCI spec treats
// arm64 without an explicit variant as equivalent to v8, and many registries
// only publish the variant form.  "aarch64" prefers plain "arm64" when it
// exists, and falls back to "arm64:v8" for the same reason.
var archAliases = map[string][]string{
	"aarch64": {"arm64", "arm64:v8"},
	"arm64":   {"arm64:v8"},
	"armhf":   {"arm"},
	"armel":   {"arm:v6"},
	"i386":    {"386"},
	"x86_64":  {"amd64"},
	"x86-64":  {"amd64"},
}

// createMultiarchAliasSymlinksWithLogger creates a .multiarch/<alias> symlink
// for every entry in archAliases whose first existing candidate directory is
// found under .multiarch/ in the repository.
//
// If the alias name already exists as a real (non-symlink) directory the
// repository has native content for that arch and no alias is created.
// Aliases whose every candidate is absent are silently skipped.
func createMultiarchAliasSymlinksWithLogger(logger *log.Entry, repo string) {
	for alias, candidates := range archAliases {
		// If the alias name is itself a real directory, native content exists
		// for that arch — no symlink alias is needed or wanted.
		aliasFullPath := filepath.Join("/", "cvmfs", repo, ".multiarch", alias)
		if lstat, err := os.Lstat(aliasFullPath); err == nil && lstat.Mode()&os.ModeSymlink == 0 {
			continue
		}

		// Pick the first candidate directory that exists.
		target := ""
		for _, candidate := range candidates {
			candidateFullPath := filepath.Join("/", "cvmfs", repo, ".multiarch", candidate)
			if _, err := os.Stat(candidateFullPath); err == nil {
				target = candidate
				break
			}
		}
		if target == "" {
			// None of the candidate arch directories exist in this repo; skip.
			continue
		}

		aliasPath := filepath.Join(".multiarch", alias)
		targetPath := filepath.Join(".multiarch", target)
		if err := cvmfs.CreateSymlinkIntoCVMFSWithLogger(logger, repo, aliasPath, targetPath); err != nil {
			l.Ensure(logger).WithFields(log.Fields{
				"alias":  aliasPath,
				"target": targetPath,
				"error":  err,
			}).Warning("Failed to create .multiarch alias symlink")
		}
	}
}

// filterManifestList returns the subset of manifests to process.
// If multiArch is true, all manifests are returned.
// If multiArch is false, only the manifest matching the native architecture is returned.
func filterManifestList(manifestList da.ManifestList, multiArch bool) []da.ManifestListItem {
	if multiArch {
		return manifestList.Manifests
	}
	nativeArch := runtime.GOARCH
	for _, entry := range manifestList.Manifests {
		// Empty architecture means single-manifest (non-multi-arch) image; always include it
		if entry.Platform.Architecture == "" || entry.Platform.Architecture == nativeArch {
			return []da.ManifestListItem{entry}
		}
	}
	// Fallback: if no match found, return all manifests
	return manifestList.Manifests
}

func ConvertWishFlat(wish WishFriendly, multiArch bool) error {
	var firstError = error(nil)

	n := notification.NewNotification(NotificationService).AddField("image_request", wish.InputName)

	nFlat := n.AddField("action", "start_flat_conversion").AddId()
	nFlat.Send()
	tFlat := time.Now()
	defer func() {
		nFlat.Elapsed(tFlat).AddField("action", "end_flat_conversion").Send()
	}()

	// it may happen at the very first round that this call returns an error, let it be
	if err := cvmfs.CreateCatalogIntoDir(wish.CvmfsRepo, ".flat"); err != nil {
		l.LogE(err).Error("Error in creating catalog inside `.flat` directory")
	}

	for _, inputImage := range wish.ExpandedTagImagesFlat {
		manifestList, err := inputImage.GetManifestList()
		if err != nil {
			l.WithImageE(err, inputImage.GetSimpleName()).Error("Error in getting the manifest list")
			if firstError == nil {
				firstError = err
			}
			continue
		}
		for _, manifestEntry := range filterManifestList(manifestList, multiArch) {
			inputImage.Manifest = &(manifestEntry.Manifest)
			nameWithArch := ""
			if multiArch {
				nameWithArch = GetNameWithArch(manifestEntry)
			}
			imageLogger := l.WithImage(imageNameWithPlatform(inputImage, manifestEntry))
			publicSymlinkPath := inputImage.GetPublicSymlinkPathWithArch(nameWithArch)
			completePubSymPath := filepath.Join("/", "cvmfs", wish.CvmfsRepo, publicSymlinkPath)
			pubDirInfo, errPub := os.Stat(completePubSymPath)

			singularityPrivatePath, err := inputImage.GetSingularityPath2(manifestEntry.Manifest)
			if err != nil {
				errF := fmt.Errorf("Error in getting the path where to save Singularity filesystem: %s", err)
				l.LogE(err).Warning(errF)
				firstError = errF
				continue
			}
			completeSingularityPriPath := filepath.Join("/", "cvmfs", wish.CvmfsRepo, singularityPrivatePath)
			priDirInfo, errPri := os.Stat(completeSingularityPriPath)

			imageLogger.WithFields(log.Fields{
				"public path":            completePubSymPath,
				"err stats public path": errPub,
				"private path":           completeSingularityPriPath,
				"err stats private path": errPri,
			}).Trace("Checking if images links are up to date")
			// no error in stating both directories
			// either the image is up to date or the image became stale
			if errPub == nil && errPri == nil {
				if os.SameFile(pubDirInfo, priDirInfo) {
					// the link is up to date
					imageLogger.Trace("Singularity image is up to date")
					continue
				}
				// delete the old pubLink
				// make a new Link to the privatePaht
				// after that skip and continue
				imageLogger.Trace("Updating singularity image symlink")
				err = cvmfs.CreateSymlinkIntoCVMFSWithLogger(imageLogger, wish.CvmfsRepo, publicSymlinkPath, singularityPrivatePath)
				if err != nil {
					errF := fmt.Errorf("Error in updating symlink for singularity image: %s", inputImage.GetSimpleName())
					imageLogger.WithField("error", errF).WithFields(
						log.Fields{"to": publicSymlinkPath, "from": singularityPrivatePath}).
						Error("Error in creating symlink")
					if firstError == nil {
						firstError = errF
					}
				}
				if err == nil {
					imageLogger.WithFields(log.Fields{
						"flat path":    completeSingularityPriPath,
						"symlink path": completePubSymPath,
					}).Info("Updated flat image symlink")
					n.Action("publish_flat_image").AddField("public_path", publicSymlinkPath).AddField("private_path", singularityPrivatePath).Send()
				}
				continue
			}

			// no error in stating the private directory, but the public one does not exists
			// we simply create the public directory
			if errPri == nil && os.IsNotExist(errPub) {
				imageLogger.Trace("Creating singularity image symlink")
				err = cvmfs.CreateSymlinkIntoCVMFSWithLogger(imageLogger, wish.CvmfsRepo, publicSymlinkPath, singularityPrivatePath)
				if err != nil {
					errF := fmt.Errorf("Error in creating symlink for singularity image: %s", publicSymlinkPath)
					imageLogger.WithField("error", errF).WithFields(
						log.Fields{"to": publicSymlinkPath, "from": singularityPrivatePath}).
						Error("Error in creating symlink")
					if firstError == nil {
						firstError = errF
					}
				}
				if err == nil {
					imageLogger.WithFields(log.Fields{
						"flat path":    completeSingularityPriPath,
						"symlink path": completePubSymPath,
					}).Info("Created flat image symlink")
					n.Action("publish_flat_image").AddField("public_path", publicSymlinkPath).AddField("private_path", singularityPrivatePath).Send()
				}
				continue
			}

			i := n.AddField("image", imageNameWithPlatform(inputImage, manifestEntry)).AddId()
			t1 := time.Now()
			i.Action("start_flat_overlay_conversion").Send()
			i = i.Action("end_flat_overlay_conversion")

			// Use cvmfs_server overlay to merge layers into a flat image
			_, err = inputImage.CreateFlatOverlayWithLogger(imageLogger, wish.CvmfsRepo)
			if err != nil {
				if firstError == nil {
					firstError = err
				}
				imageLogger.WithField("error", err).Error("Error in creating the flat overlay")
				i.Error(err).Elapsed(t1).Send()
				continue
			}

			ociImage, err := inputImage.GetOCIImage()
			if err != nil {
				if firstError == nil {
					firstError = err
				}
				l.LogE(err).Error("Error in getting the OCI image configuration")
				i.Error(err).Elapsed(t1).Send()
				continue
			}
			// we create the singularity dotfiles inside the flat overlay result
			err = cvmfs.WithinTransaction(wish.CvmfsRepo,
				func() error {
					if err := singularity.MakeBaseEnv(completeSingularityPriPath); err != nil {
						imageLogger.WithField("error", err).Error("Error in creating the base singularity environment")
						return err
					}
					if err := singularity.InsertRunScript(completeSingularityPriPath, ociImage); err != nil {
						imageLogger.WithField("error", err).Error("Error in inserting the singularity runscript")
						return err
					}
					if err := singularity.InsertEnv(completeSingularityPriPath, ociImage); err != nil {
						imageLogger.WithField("error", err).Error("Error in inserting the singularity environment")
						return err
					}
					return nil
				})

			if err != nil {
				if firstError == nil {
					firstError = err
				}
				imageLogger.WithField("error", err).Error("Error in creating the dotfile inside the flat directory")
				i.Error(err).Elapsed(t1).Send()
				continue
			}
			// we create the public link

			err = cvmfs.CreateSymlinkIntoCVMFSWithLogger(imageLogger, wish.CvmfsRepo, publicSymlinkPath, singularityPrivatePath)
			if err != nil {
				errF := fmt.Errorf("Error in creating symlink for singularity image: %s", inputImage.GetSimpleName())
				imageLogger.WithField("error", errF).WithFields(
					log.Fields{"to": publicSymlinkPath, "from": singularityPrivatePath}).
					Error("Error in creating symlink")
				if firstError == nil {
					firstError = errF
				}
				i.Error(err).Elapsed(t1).Send()
				continue
			}
			i.Error(err).Elapsed(t1).Send()
			if err == nil {
				imageLogger.WithFields(log.Fields{
					"flat path":    completeSingularityPriPath,
					"symlink path": completePubSymPath,
				}).Info("Created flat image and symlink")
				n.Action("publish_flat_image").AddField("public_path", publicSymlinkPath).AddField("private_path", singularityPrivatePath).Send()
			}
			continue

		}

		// After processing all architectures for this image, create (or update)
		// the user-facing variant symlink at <registry>/<repo>:<tag>.  The
		// symlink contains a $(CVMFS_ARCH:-<native>) expression so that the
		// CVMFS client resolves it to the correct architecture-specific flat
		// image at runtime without any server-side changes.
		if multiArch {
			variantPath := inputImage.GetPublicSymlinkPath()
			variantTarget := inputImage.GetVariantSymlinkTarget(runtime.GOARCH)
			variantLogger := l.WithImage(inputImage.GetSimpleName())
			variantLogger.WithFields(log.Fields{
				"symlink path": variantPath,
				"target":       variantTarget,
			}).Debug("Creating multiarch variant symlink")
			if vErr := cvmfs.CreateVariantSymlinkIntoCVMFSWithLogger(variantLogger, wish.CvmfsRepo, variantPath, variantTarget); vErr != nil {
				variantLogger.WithField("error", vErr).Warning("Failed to create variant symlink for multiarch image")
				if firstError == nil {
					firstError = vErr
				}
			}
		}
	}
	if multiArch {
		createMultiarchAliasSymlinksWithLogger(nil, wish.CvmfsRepo)
	}
	return firstError
}

// ConvertWishPodman publishes a podman additional image store entry for each
// image in wish into /cvmfs/<repo>/podmanStore/ using a single CVMFS
// transaction per image.  It requires that the flat image has already been
// published (i.e. skipFlat was not set).  The multiArch parameter must match
// what was passed to ConvertWishFlat so that only architectures with a flat
// image are published.
func ConvertWishPodman(wish WishFriendly, multiArch bool) error {
	var firstError error
	for _, inputImage := range wish.ExpandedTagImagesFlat {
		manifestList, err := inputImage.GetManifestList()
		if err != nil {
			l.WithImageE(err, inputImage.GetSimpleName()).Error("Error getting manifest list for podman store")
			if firstError == nil {
				firstError = err
			}
			continue
		}
		for _, manifestEntry := range filterManifestList(manifestList, multiArch) {
			inputImage.Manifest = &(manifestEntry.Manifest)
			imageLogger := l.WithImage(inputImage.GetSimpleName())
			if err := inputImage.PublishPodmanStoreWithLogger(imageLogger, wish.CvmfsRepo); err != nil {
				imageLogger.WithField("error", err).Error("Error publishing podman store entry")
				if firstError == nil {
					firstError = err
				}
			}
		}
	}
	return firstError
}

func ConvertWishDocker(wish WishFriendly) (err error) {
	inputImage := wish.InputImage
	if inputImage == nil {
		err = fmt.Errorf("error in parsing the input image, got a null image")
		l.LogE(err).WithFields(log.Fields{"input image": wish.InputName}).
			Error("Null image, should not happen")
		return
	}
	outputImage := wish.OutputImage
	if outputImage == nil {
		err = fmt.Errorf("error in parsing the output image, got a null image")
		l.LogE(err).WithFields(log.Fields{"output image": wish.OutputName}).
			Error("Null image, should not happen")
		return
	}
	var firstError error
	for _, expandedImgTag := range wish.ExpandedTagImagesLayer {
		imageLogger := l.WithImage(expandedImgTag.GetSimpleName())
		tag := expandedImgTag.Tag
		outputWithTag := outputImageForExpandedTag(inputImage, outputImage, tag)

		manifestPath := filepath.Join("/", "cvmfs", wish.CvmfsRepo, ".metadata", expandedImgTag.GetSimpleName(), "manifest.json")
		if _, err := os.Stat(manifestPath); os.IsNotExist(err) {
			imageLogger.Trace("Layers not downloaded yet, not converting for docker")
			continue
		}
		manifest, err := expandedImgTag.GetManifest()
		if err != nil {
			return err
		}
		layerLocations := make(map[string]string)
		for _, layer := range manifest.Layers {
			layerDigest := strings.Split(layer.Digest, ":")[1]
			layerPath := cvmfs.LayerRootfsPath(wish.CvmfsRepo, layerDigest)
			layerLocations[layer.Digest] = layerPath
		}
		err = CreateThinImageWithLogger(imageLogger, manifest, layerLocations, *expandedImgTag, outputWithTag)
		if err != nil && firstError == nil {
			firstError = err
		}
		err = PushImageToRegistryWithLogger(imageLogger.WithField("thin_image", outputWithTag.GetSimpleName()), outputWithTag)
		if err != nil && firstError == nil {
			firstError = err
		}
	}
	return firstError
}

func outputImageForExpandedTag(inputImage, outputImage *Image, expandedTag string) Image {
	outputWithTag := *outputImage
	if inputImage.TagWildcard {
		outputWithTag.Tag = expandedTag
	}
	return outputWithTag
}

func outputRepositoryForImport(outputImage Image) string {
	outputRepository := outputImage
	outputRepository.Tag = ""
	return outputRepository.GetSimpleName()
}

func ConvertWish(wish WishFriendly, convertAgain, forceDownload, multiArch bool, maxConcurrentDownloads int) (summary ConversionSummary, err error) {
	err = cvmfs.CreateCatalogIntoDir(wish.CvmfsRepo, constants.SubDirInsideRepo)
	if err != nil {
		l.LogE(err).WithFields(log.Fields{
			"directory": constants.SubDirInsideRepo}).Error(
			"Impossible to create subcatalog in the directory.")
	}
	var firstError error
	for _, expandedImgTag := range wish.ExpandedTagImagesLayer {
		imageSummary, err := convertInputOutput(expandedImgTag, wish.CvmfsRepo, convertAgain, forceDownload, multiArch, maxConcurrentDownloads)
		summary.Merge(imageSummary)
		if err != nil && firstError == nil {
			firstError = err
		}
	}
	return summary, firstError
}

func convertInputOutput(inputImage *Image, repo string, convertAgain, forceDownload bool, multiArch bool, maxConcurrentDownloads int) (ConversionSummary, error) {
	manifestList, err := inputImage.GetManifestList()
	if err != nil {
		l.WithImageE(err, inputImage.GetSimpleName()).Error("Error in getting the manifest list")
		return ConversionSummary{}, err
	}
	summary := ConversionSummary{}
	var firstError error
	for _, manifestEntry := range filterManifestList(manifestList, multiArch) {
		inputImage.Manifest = &(manifestEntry.Manifest)
		nameWithArch := GetNameWithArch(manifestEntry)
		nameWithArch = filepath.Join(nameWithArch, inputImage.GetSimpleName())
		imageLabel := imageNameWithPlatform(inputImage, manifestEntry)
		result, err := convertInputOutput2(inputImage, imageLabel, nameWithArch, repo, convertAgain, forceDownload, maxConcurrentDownloads)
		if err == nil {
			summary.Add(result, imageLabel)
		}
		if err != nil {
			if firstError == nil {
				firstError = err
			}
		}
	}
	return summary, firstError
}

func convertInputOutput2(inputImage *Image, imageLabel, nameWithArch, repo string, convertAgain, forceDownload bool, maxConcurrentDownloads int) (result ConversionResult, err error) {
	result = ConversionUnknown
	logger := l.WithImage(imageLabel)
	path := filepath.Join("/", "cvmfs", repo, ".metadata")
	manifest, _ := inputImage.GetManifest()

	manifestPath := filepath.Join(path, nameWithArch, "manifest.json")
	alreadyConverted := AlreadyConvertedWithLogger(logger, manifestPath, manifest.Config.Digest)
	result = alreadyConverted

	// Classify layers into those already present in CVMFS and those that need
	// to be downloaded and ingested, mirroring the forceDownload logic in
	// GetLayersWithLogger so the message accurately reflects what will happen.
	var layersToConvert, layersAlreadyConverted []string
	for _, layer := range manifest.Layers {
		if layer.MediaType == "application/vnd.docker.image.rootfs.foreign.diff.tar.gzip" {
			continue
		}
		digest := strings.Split(layer.Digest, ":")[1]
		shortDigest := digest
		if len(shortDigest) > 12 {
			shortDigest = shortDigest[:12]
		}
		layerPath := cvmfs.LayerRootfsPath(repo, digest)
		if !forceDownload {
			if _, statErr := os.Stat(layerPath); statErr == nil {
				layersAlreadyConverted = append(layersAlreadyConverted, shortDigest)
				continue
			}
		}
		layersToConvert = append(layersToConvert, shortDigest)
	}
	logger.WithFields(log.Fields{
		"layers to convert": layersToConvert,
		"already converted": layersAlreadyConverted,
	}).Info("Starting layer conversion")

	if alreadyConverted == ConversionMatch {
		if !convertAgain {
			return result, nil
		}
	}

	layersChanell := make(chan downloadedLayer, 10)
	manifestChanell := make(chan string, 1)
	stopGettingLayers := make(chan bool, 1)
	noErrorInConversion := make(chan bool, 1)

	n := notification.NewNotification(NotificationService)
	n = n.AddField("image", imageLabel)

	go func() {
		noErrors := true
		defer func() {
			noErrorInConversion <- noErrors
			stopGettingLayers <- true
			close(stopGettingLayers)
		}()

		for layer := range layersChanell {
			layerDigest := strings.Split(layer.Name, ":")[1]
			layerLogger := logger.WithField("layer", layer.Name)

			layerLogger.Info("Start ingesting layer into CVMFS")

			ln := n.AddField("layer", layerDigest).AddId()
			ln.Action("start_layer_conversion").Send()

			t1 := time.Now()
			err = layer.IngestIntoCVMFSWithLogger(layerLogger, repo)

			ln.Elapsed(t1).Action("end_layer_conversion").Error(err).SizeBytes(layer.GetSize()).Send()

			if err != nil {
				layerLogger.WithField("error", err).Error("Error in ingesting the layer in cvmfs")
				noErrors = false
			}
			if err == nil {
				layerLogger.WithFields(log.Fields{
					"duration": time.Since(t1).Round(time.Millisecond),
					"size":     fmt.Sprintf("%.1f MB", float64(layer.GetSize())/1e6),
				}).Info("Layer ingest into CVMFS finished")
				n.Action("publish_layer").AddField("layer_digest", layerDigest).Send()
			}
			layerLogger.Trace("Finished ingesting the file")

			layer.Close()
		}
		logger.Trace("Finished pushing the layers into CVMFS")
	}()
	// we create a temp directory for all the files needed, when this function finish we can remove the temp directory cleaning up
	tmpDir, err := temp.UserDefinedTempDir("", "conversion")
	if err != nil {
		logger.WithField("error", err).Error("Error in creating a temporary directory for all the files")
		return
	}
	defer os.RemoveAll(tmpDir)

	// this will start to feed the above goroutine by writing into layersChanell
	err = inputImage.GetLayersWithLogger(logger, manifest, layersChanell, manifestChanell, stopGettingLayers, tmpDir, maxConcurrentDownloads, repo, forceDownload)
	if err != nil {
		logger.WithField("error", err).Error("Error in getting layers")
		return result, err
	}

	// Collect all layer digests from the manifest for backlink saving,
	// including layers that were skipped because they already existed.
	var layerDigests []string
	for _, layer := range manifest.Layers {
		if layer.MediaType == "application/vnd.docker.image.rootfs.foreign.diff.tar.gzip" {
			continue
		}
		layerDigests = append(layerDigests, strings.Split(layer.Digest, ":")[1])
	}

	// we wait for the goroutines to finish
	// and if there was no error we conclude everything writing the manifest into the repository
	noErrorInConversionValue := <-noErrorInConversion

	err = cvmfs.SaveLayersBacklinkWithLogger(logger, repo, manifest, layerDigests)
	if err != nil {
		logger.WithField("error", err).Error("Error in saving the backlinks")
		noErrorInConversionValue = false
	}

	if noErrorInConversionValue {
		manifestPath2 := filepath.Join(".metadata", nameWithArch, "manifest.json")
		errIng := cvmfs.PublishToCVMFSWithLogger(logger, repo, manifestPath2, <-manifestChanell)
		if errIng != nil {
			logger.WithField("error", errIng).Error("Error in storing the manifest in the repository")
			return result, errIng
		}

		var errRemoveSchedule error
		if alreadyConverted == ConversionNotMatch {
			logger.Trace("Image already converted, but it does not match the manifest, adding it to the remove scheduler")
			errRemoveSchedule = cvmfs.AddManifestToRemoveSchedulerWithLogger(logger, repo, manifest)
			if errRemoveSchedule != nil {
				logger.WithField("error", errRemoveSchedule).Warning("Error in adding the image to the remove schedule")
				return result, errRemoveSchedule
			}
		}
		logger.Trace("Conversion completed")
		return result, nil
	}

	logger.Warn("Some error during the conversion, we are not storing it into the database")
	return ConversionUnknown, fmt.Errorf("error ingesting one or more layers for image %s", inputImage.GetSimpleName())
}

func CreateThinImage(manifest da.Manifest, layerLocations map[string]string, inputImage, outputImage Image) (err error) {
	return CreateThinImageWithLogger(nil, manifest, layerLocations, inputImage, outputImage)
}

func CreateThinImageWithLogger(logger *log.Entry, manifest da.Manifest, layerLocations map[string]string, inputImage, outputImage Image) (err error) {
	logger = l.Ensure(logger)
	thin, err := da.MakeThinImage(manifest, layerLocations, inputImage.WholeName())
	if err != nil {
		return
	}

	thinJson, err := json.MarshalIndent(thin, "", "  ")
	if err != nil {
		return
	}
	var imageTar bytes.Buffer
	tarFile := tar.NewWriter(&imageTar)
	header := &tar.Header{Name: "thin.json", Mode: 0644, Size: int64(len(thinJson))}
	err = tarFile.WriteHeader(header)
	if err != nil {
		return
	}
	_, err = tarFile.Write(thinJson)
	if err != nil {
		return
	}
	err = tarFile.Close()
	if err != nil {
		return
	}

	dockerClient, err := NewDockerClient()
	if err != nil {
		return
	}

	changes, _ := inputImage.GetChanges()
	image := dockerImage.ImportSource{
		Source:     bytes.NewBuffer(imageTar.Bytes()),
		SourceName: "-",
	}
	importOptions := dockerImage.ImportOptions{
		Tag:     outputImage.Tag,
		Message: "",
		Changes: changes,
	}
	importResult, err := dockerClient.ImageImport(
		context.Background(),
		image,
		outputRepositoryForImport(outputImage),
		importOptions)
	if err != nil {
		logger.WithField("error", err).Error("Error in image import")
		return
	}
	defer importResult.Close()
	logger.Trace("Created the image in the local docker daemon")

	return nil
}

func PushImageToRegistry(outputImage Image) (err error) {
	return PushImageToRegistryWithLogger(nil, outputImage)
}

func PushImageToRegistryWithLogger(logger *log.Entry, outputImage Image) (err error) {
	logger = l.Ensure(logger)
	// the authentication must be provided for the ImagePush api,
	// even if the documentation says otherwise
	password, err := GetPassword()
	if err != nil {
		return err
	}
	authStruct := struct {
		Username string
		Password string
	}{
		Username: outputImage.User,
		Password: password,
	}
	authBytes, _ := json.Marshal(authStruct)
	authCredential := base64.StdEncoding.EncodeToString(authBytes)
	pushOptions := dockerImage.PushOptions{
		RegistryAuth: authCredential,
	}
	dockerClient, err := NewDockerClient()
	if err != nil {
		return err
	}
	res, errImgPush := dockerClient.ImagePush(
		context.Background(),
		outputImage.GetSimpleName(),
		pushOptions)
	if errImgPush != nil {
		err = fmt.Errorf("Error in pushing the image: %s", errImgPush)
		return err
	}
	b, _ := ioutil.ReadAll(res)
	logger.WithField("action", "prepared thin-image manifest").Trace(string(b))
	defer res.Close()
	// here is possible to use the result of the above ReadAll to have
	// informantion about the status of the upload.
	_, errReadDocker := ioutil.ReadAll(res)
	if err != nil {
		logger.WithField("error", errReadDocker).Warning("Error in reading the status from docker")
	}
	logger.Trace("Finished pushing the image to the registry")
	return
}

// NewDockerClient creates a Docker client that negotiates the API version with
// the daemon to stay compatible across Docker releases.
func NewDockerClient() (*client.Client, error) {
	return client.NewClientWithOpts(
		client.FromEnv,
		client.WithAPIVersionNegotiation(),
	)
}

func StoreLayerInfo(CVMFSRepo string, layerDigest string, r ReadHashCloseSizer) (err error) {
	return StoreLayerInfoWithLogger(nil, CVMFSRepo, layerDigest, r)
}

func StoreLayerInfoWithLogger(logger *log.Entry, CVMFSRepo string, layerDigest string, r ReadHashCloseSizer) (err error) {
	logger = l.Ensure(logger)
	logger.WithField("action", "ingesting layers.json").Trace("Store layer information in .layers")
	layersdata := []LayerInfo{}
	layerInfoPath := filepath.Join(cvmfs.LayerMetadataPath(CVMFSRepo, layerDigest), "layers.json")

	diffID := fmt.Sprintf("%x", r.Sum(nil))
	size := r.GetSize()
	created := time.Now()
	layerinfo := LayerInfo{
		ID:                   diffID,
		Created:              created,
		CompressedDiffDigest: "sha256:" + layerDigest,
		UncompressedDigest:   "sha256:" + diffID,
		UncompressedSize:     size,
	}
	layersdata = append(layersdata, layerinfo)

	jsonLayerInfo, err := json.MarshalIndent(layersdata, "", " ")
	if err != nil {
		logger.WithField("error", err).Error("Error in marshaling json data for layers.json")
		return err
	}

	err = cvmfs.WriteDataToCvmfsWithLogger(logger, CVMFSRepo, cvmfs.TrimCVMFSRepoPrefix(layerInfoPath), jsonLayerInfo)
	if err != nil {
		logger.WithField("error", err).Error("Error in writing layers.json file")
		return err
	}
	return
}

func AlreadyConverted(manifestPath, reference string) ConversionResult {
	return AlreadyConvertedWithLogger(nil, manifestPath, reference)
}

func AlreadyConvertedWithLogger(logger *log.Entry, manifestPath, reference string) ConversionResult {
	logger = l.Ensure(logger)
	manifestStat, err := os.Stat(manifestPath)
	if os.IsNotExist(err) {
		logger.Trace("Manifest not existing")
		return ConversionNotFound
	}
	if !manifestStat.Mode().IsRegular() {
		logger.Trace("Manifest not a regular file")
		return ConversionNotFound
	}

	manifestFile, err := os.Open(manifestPath)
	if err != nil {
		logger.WithField("error", err).Trace("Error in opening the manifest")
		return ConversionNotFound
	}
	defer manifestFile.Close()

	bytes, _ := ioutil.ReadAll(manifestFile)

	var manifest da.Manifest
	err = json.Unmarshal(bytes, &manifest)
	if err != nil {
		logger.WithField("error", err).Warning("Error in unmarshaling the manifest")
		return ConversionNotFound
	}
	if manifest.Config.Digest == reference {
		return ConversionMatch
	}
	return ConversionNotMatch
}

func GetPassword() (string, error) {
	envVar := "DUCC_OUTPUT_REGISTRY_PASS"
	pass := os.Getenv(envVar)
	if pass == "" {
		err := fmt.Errorf(
			"Env variable (%s) storing the password to push thin images is not set", envVar)
		return "", err
	}
	return pass, nil
}
