package sif

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/cvmfs/ducc/sif/squashfs"
)

func FuzzImage(f *testing.F) {
	if _, err := exec.LookPath("mksquashfs"); err == nil {
		dir := f.TempDir()
		src := filepath.Join(dir, "src")
		os.MkdirAll(filepath.Join(src, "d"), 0755)
		os.WriteFile(filepath.Join(src, "d/f"), bytes.Repeat([]byte("x"), 5000), 0644)
		os.WriteFile(filepath.Join(src, "small"), []byte("s"), 0644)
		os.Symlink("d/f", filepath.Join(src, "l"))
		for _, comp := range []string{"gzip", "zstd", "xz", "lz4"} {
			img := filepath.Join(dir, comp+".sqfs")
			mksquashfs(f, src, img, "-comp", comp)
			if data, err := os.ReadFile(img); err == nil {
				f.Add(data)
				sifPath := filepath.Join(dir, comp+".sif")
				writeSIF(f, img, sifPath, FsSquash, PartPrimSys)
				if s, err := os.ReadFile(sifPath); err == nil {
					f.Add(s)
				}
				upper := filepath.Join(dir, comp+"-upper.sqfs")
				mklayer(f, src, upper, "wh c 644 0 0 0 0", "d x trusted.overlay.opaque=y")
				writeOCISIF(f, newOCIParts(f, []string{img, upper}, testConfig()), sifPath)
				if s, err := os.ReadFile(sifPath); err == nil {
					f.Add(s)
				}
			}
		}
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := bytes.NewReader(data)
		im, err := Parse(r, int64(len(data)))
		if err != nil {
			return
		}
		opts := DefaultStreamOptions()
		opts.Workers = 2
		if im.OCI {
			o, err := im.ReadOCI(r)
			if err != nil {
				return
			}
			if t, _, err := BuildOCITree(r, o, 1<<14); err == nil {
				StreamTree(context.Background(), io.Discard, t, opts)
			}
			return
		}
		fs, err := squashfs.Open(im.Partition(r), im.PartSize)
		if err != nil {
			return
		}
		ix, err := fs.BuildIndex(1 << 14)
		if err != nil {
			return
		}
		Stream(context.Background(), io.Discard, fs, ix, opts)
	})
}
