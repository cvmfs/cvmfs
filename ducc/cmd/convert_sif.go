package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/cvmfs/ducc/lib"
	l "github.com/cvmfs/ducc/log"
	"github.com/cvmfs/ducc/notification"
	"github.com/cvmfs/ducc/sif"
)

var (
	sifName             string
	sifDigest           string
	sifForce            bool
	sifAttempts         int
	sifAllowNoSingDir   bool
	sifEmitTar          string
	sifDryRun           bool
	sifHeaders          []string
	sifChunkMiB         int
	sifCacheMiB         int
	sifReadahead        int
	sifWindowMiB        int
	sifFragCache        int
	sifWorkers          int
	sifCatalogMaxWeight int
	sifCatalogMinWeight int
	sifMaxEntries       int
	sifContiguous       bool
)

func init() {
	f := convertSIFCmd.Flags()
	f.StringVar(&sifName, "name", "", "public path below sif/ (e.g. lhcb/analysis:v3)")
	f.StringVar(&sifDigest, "digest", "", "override the image ID (sha256:HEX); default is a hash of the SIF header and descriptors")
	f.BoolVar(&sifForce, "force", false, "ingest again even if the image is already present")
	f.IntVarP(&sifAttempts, "attempts", "r", 1, "number of attempts for the whole conversion")
	f.BoolVar(&sifAllowNoSingDir, "allow-missing-singularity-d", false, "publish images without /.singularity.d")
	f.StringVar(&sifEmitTar, "emit-tar", "", "write the tar stream to FILE (- for stdout) and exit; no repository access")
	f.BoolVar(&sifDryRun, "dry-run", false, "parse and index the image, print a summary and exit; no repository access")
	f.StringArrayVar(&sifHeaders, "http-header", nil, "extra HTTP header 'Name: value' (repeatable)")
	f.IntVar(&sifChunkMiB, "chunk-size", 8, "HTTP range request size in MiB")
	f.IntVar(&sifCacheMiB, "cache-size", 64, "HTTP chunk cache in MiB")
	f.IntVar(&sifReadahead, "readahead", 4, "chunks fetched ahead on sequential access")
	f.IntVar(&sifWindowMiB, "decompress-window", 64, "decompressed data buffered ahead of the tar writer, in MiB")
	f.IntVar(&sifFragCache, "fragment-cache", 64, "decompressed fragment blocks kept in memory")
	f.IntVar(&sifWorkers, "workers", 0, "decompression workers (0: number of CPUs)")
	f.IntVar(&sifCatalogMaxWeight, "catalog-max-weight", 100000, "split off nested catalogs above this many entries (0 disables)")
	f.IntVar(&sifCatalogMinWeight, "catalog-min-weight", 1000, "do not create nested catalogs smaller than this")
	f.IntVar(&sifMaxEntries, "max-entries", 10_000_000, "refuse images with more entries")
	f.BoolVar(&sifContiguous, "contiguous-catalogs", false, "upload each nested catalog as soon as it is complete (needs cvmfs_server ingest --contiguous-catalogs)")
	rootCmd.AddCommand(convertSIFCmd)
}

func sifSourceOptions() (sif.SourceOptions, error) {
	o := sif.DefaultSourceOptions()
	o.ChunkSize = int64(sifChunkMiB) << 20
	o.CacheSize = int64(sifCacheMiB) << 20
	o.Readahead = sifReadahead
	o.Headers = http.Header{}
	for _, h := range sifHeaders {
		k, v, ok := strings.Cut(h, ":")
		if !ok {
			return o, fmt.Errorf("invalid --http-header %q, want 'Name: value'", h)
		}
		o.Headers.Add(strings.TrimSpace(k), strings.TrimSpace(v))
	}
	return o, nil
}

func sifStreamOptions() sif.StreamOptions {
	o := sif.DefaultStreamOptions()
	if sifWorkers > 0 {
		o.Workers = sifWorkers
	}
	o.WindowBytes = sifWindowMiB << 20
	o.FragmentCache = sifFragCache
	o.CatalogMaxWeight = sifCatalogMaxWeight
	o.CatalogMinWeight = sifCatalogMinWeight
	o.ContiguousCatalogs = sifContiguous
	return o
}

var convertSIFCmd = &cobra.Command{
	Use:   "convert-sif <sif file or URL> [<cvmfs repository>]",
	Short: "Publish the root filesystem of a SIF image as a flat image",
	Long: `Streams the squashfs partition of a SIF image into 'cvmfs_server ingest'
without unpacking it locally. The source can be a local path or an http(s)
URL served with range-request support. The image is stored at
.flat/<xx>/<id> and linked from sif/<name>.`,
	Args: cobra.RangeArgs(1, 2),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		defer signal.Stop(sigChan)
		go func() {
			if sig, ok := <-sigChan; ok {
				l.Log().WithField("signal", sig).Info("Received signal, aborting SIF conversion")
				cancel()
			}
		}()

		sopts, err := sifSourceOptions()
		if err != nil {
			return err
		}
		source := args[0]
		// Most of the heap is short-lived chunk and block buffers; without a
		// limit the GC lets the heap grow to twice the configured caches.
		if os.Getenv("GOMEMLIMIT") == "" {
			debug.SetMemoryLimit(int64(sifCacheMiB+sifWindowMiB+(sifReadahead+1)*sifChunkMiB)<<20 + 256<<20)
		}

		if sifDryRun || sifEmitTar != "" {
			return sifInspectOnly(ctx, source, sopts)
		}
		if len(args) != 2 {
			return fmt.Errorf("a cvmfs repository is required unless --dry-run or --emit-tar is given")
		}
		if err := sif.ValidateName(sifName); err != nil {
			return err
		}
		AliveMessage()

		opts := sif.ConvertOptions{
			Source: source, Repo: args[1], Name: sifName, Digest: sifDigest,
			Force: sifForce, AllowMissingSingularityDir: sifAllowNoSingDir,
			MaxEntries: sifMaxEntries, Stream: sifStreamOptions(), SourceOpts: sopts,
			Notification: notification.NewNotification(lib.NotificationService),
			Logger:       l.Log(),
		}
		var res sif.Result
		for attempt := 1; attempt <= sifAttempts; attempt++ {
			res, err = sif.Convert(ctx, opts)
			if err == nil || ctx.Err() != nil {
				break
			}
			l.Log().WithFields(log.Fields{"attempt": attempt, "error": err}).Warn("SIF conversion failed")
		}
		if err != nil {
			return err
		}
		fmt.Printf("%s -> %s\n", res.PublicPath, res.FlatPath)
		return nil
	},
}

func sifInspectOnly(ctx context.Context, source string, sopts sif.SourceOptions) error {
	sopts.Logger = l.Log()
	t := time.Now()
	in, err := sif.Inspect(ctx, source, sifDigest, sifMaxEntries, sopts)
	if err != nil {
		return err
	}
	defer in.Src.Close()
	indexTime := time.Since(t)

	var st sif.StreamStats
	if sifEmitTar != "" {
		var w io.Writer = os.Stdout
		if sifEmitTar != "-" {
			f, err := os.Create(sifEmitTar)
			if err != nil {
				return err
			}
			defer f.Close()
			w = f
		}
		sopts := sifStreamOptions()
		sopts.Logger = l.Log()
		if st, err = sif.Stream(ctx, w, in.FS, in.Index, sopts); err != nil {
			return err
		}
	}
	res := sif.Result{ImageID: in.ID, IDKind: in.IDKind, Stream: st, Source: in.Src.Stats()}
	res.FlatPath = sif.FlatPath(in.ID)
	if sifName != "" {
		res.PublicPath = sif.PublicPath(sifName)
	}
	summary := struct {
		sif.Provenance
		FlatPath   string  `json:"flat_path"`
		PublicPath string  `json:"public_path,omitempty"`
		Entries    int     `json:"index_entries"`
		IndexTime  float64 `json:"index_seconds"`
		StreamTime float64 `json:"stream_seconds,omitempty"`
		FetchRatio float64 `json:"fetch_ratio"`
	}{
		Provenance: sif.NewProvenance(sif.ConvertOptions{Source: source}, in, &res),
		FlatPath:   res.FlatPath, PublicPath: res.PublicPath,
		Entries: len(in.Index.Entries), IndexTime: indexTime.Seconds(),
		StreamTime: st.Elapsed.Seconds(),
		FetchRatio: float64(res.Source.BytesFetched) / float64(in.Image.PartSize),
	}
	out := os.Stdout
	if sifEmitTar == "-" {
		out = os.Stderr
	}
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	return enc.Encode(summary)
}
