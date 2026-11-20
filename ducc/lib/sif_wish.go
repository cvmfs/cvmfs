package lib

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/cvmfs/ducc/cvmfs"
	l "github.com/cvmfs/ducc/log"
	"github.com/cvmfs/ducc/notification"
	"github.com/cvmfs/ducc/sif"
)

// SIFWish is one sif_input entry of a recipe.
type SIFWish struct {
	Source string `yaml:"source"`
	Name   string `yaml:"name"`
	Digest string `yaml:"digest"`
}

var sifWishKeys = map[string]bool{"source": true, "name": true, "digest": true}

// UnmarshalYAML rejects unknown keys: a misspelt "digest" would otherwise
// silently drop a pin.
func (w *SIFWish) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var raw map[string]interface{}
	if err := unmarshal(&raw); err != nil {
		return fmt.Errorf("sif_input entries must be maps with source, name and optional digest: %w", err)
	}
	var unknown []string
	for k := range raw {
		if !sifWishKeys[k] {
			unknown = append(unknown, k)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return fmt.Errorf("sif_input: unknown key(s) %s", strings.Join(unknown, ", "))
	}
	type plain SIFWish
	return unmarshal((*plain)(w))
}

func (w SIFWish) PublicPath() string { return sif.PublicPath(w.Name) }

func (o YamlSIFOptionsV1) streamOptions() sif.StreamOptions {
	s := sif.DefaultStreamOptions()
	s.ContiguousCatalogs = o.ContiguousCatalogs
	if o.CatalogMaxWeight != nil {
		s.CatalogMaxWeight = *o.CatalogMaxWeight
	}
	if o.CatalogMinWeight != nil {
		s.CatalogMinWeight = *o.CatalogMinWeight
	}
	if o.Workers > 0 {
		s.Workers = o.Workers
	}
	return s
}

func (o YamlSIFOptionsV1) sourceOptions() sif.SourceOptions {
	s := sif.DefaultSourceOptions()
	if o.CacheSizeMiB > 0 {
		s.CacheSize = int64(o.CacheSizeMiB) << 20
	}
	return s
}

// The SIF header and an apptainer-default descriptor table (48 * 585 bytes
// at offset 4096) fit into one such range request, instead of the 8 MiB
// chunk the ingest uses.
const sifProbeChunk = 64 << 10

func identifySIF(ctx context.Context, w SIFWish, logger *log.Entry) (string, error) {
	opts := sif.SourceOptions{ChunkSize: sifProbeChunk, CacheSize: 2 * sifProbeChunk, Logger: logger}
	in, err := sif.Identify(ctx, w.Source, w.Digest, opts)
	if err != nil {
		return "", err
	}
	in.Src.Close()
	return in.ID, nil
}

func readRepoLink(repo, path string) string {
	target, _ := os.Readlink(filepath.Join("/", "cvmfs", repo, path))
	return target
}

// ConvertSIFWish publishes one sif_input entry. An unchanged image costs the
// header reads of identifySIF plus two stat calls in the mounted repository;
// the full sif.Convert, which opens the source again with ingest-sized
// chunks, only runs when the image or the link has to change.
func ConvertSIFWish(ctx context.Context, repo string, w SIFWish, o YamlSIFOptionsV1, force bool, n *notification.Notification) (ConversionSummary, error) {
	summary := ConversionSummary{}
	public := w.PublicPath()
	logger := l.Log().WithFields(log.Fields{"source": w.Source, "public": public, "repo": repo})
	if !force {
		id, err := identifySIF(ctx, w, logger)
		if err != nil {
			return summary, err
		}
		if sif.Published(repo, w.Name, id) {
			logger.WithField("image_id", id).Info("SIF image unchanged, nothing to do")
			summary.Add(ConversionMatch, public)
			return summary, nil
		}
	}
	before := readRepoLink(repo, public)
	res, err := sif.Convert(ctx, sif.ConvertOptions{
		Source: w.Source, Repo: repo, Name: w.Name, Digest: w.Digest, Force: force,
		Stream: o.streamOptions(), SourceOpts: o.sourceOptions(),
		Notification: n, Logger: logger,
	})
	if err != nil {
		return summary, err
	}
	after := readRepoLink(repo, public)
	switch {
	case before == "":
		summary.Add(ConversionNotFound, public)
	case res.Skipped && before == after:
		summary.Add(ConversionMatch, public)
	default:
		summary.Add(ConversionNotMatch, public)
	}
	return summary, nil
}

// ConvertSIFWishes converts the sif_input entries of a recipe one after the
// other; a failing entry is recorded and does not stop the rest. Errors of
// sources matched by ignored are logged only. between runs after each entry.
func ConvertSIFWishes(ctx context.Context, recipe Recipe, force bool, ignored func(string) bool, between func()) (ConversionSummary, []string) {
	summary := ConversionSummary{}
	var errs []string
	for _, w := range recipe.SIFWishes {
		fields := log.Fields{"sif source": w.Source, "public": w.PublicPath(), "repository": recipe.Repo}
		l.Log().WithFields(fields).Info("Start conversion of SIF wish")
		var n *notification.Notification
		if NotificationService != nil {
			n = notification.NewNotification(NotificationService)
		}
		s, err := ConvertSIFWish(ctx, recipe.Repo, w, recipe.SIFOptions, force, n)
		summary.Merge(s)
		if err != nil {
			if ignored != nil && ignored(w.Source) {
				l.LogE(err).WithFields(fields).Warning("Error in converting SIF wish, but source is in ignoreErrors list")
			} else {
				l.LogE(err).WithFields(fields).Error("Error in converting SIF wish, going on")
				errs = append(errs, fmt.Sprintf("[%s] sif: %s", w.Source, err))
				summary.AddFailed(w.PublicPath())
			}
		}
		if between != nil {
			between()
		}
		if ctx.Err() != nil {
			break
		}
	}
	return summary, errs
}

// DeleteSIFFromCVMFS removes the public link sif/<name>, which makes the
// image and its provenance file eligible for the next garbage collection.
// Returns false if there is no such link in the mounted repository.
func DeleteSIFFromCVMFS(repo, name string, dryRun bool) (bool, error) {
	if err := sif.ValidateName(name); err != nil {
		return false, err
	}
	public := sif.PublicPath(name)
	abs := filepath.Join("/", "cvmfs", repo, public)
	fi, err := os.Lstat(abs)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	// sif/<name> can be a directory holding other names (sif/a/b).
	if fi.Mode()&os.ModeSymlink == 0 {
		return false, fmt.Errorf("%s is not a symlink", abs)
	}
	if dryRun {
		fmt.Printf("[dry-run] Would delete symlink: %s\n", abs)
		return true, nil
	}
	return true, cvmfs.IngestDelete(repo, public)
}
