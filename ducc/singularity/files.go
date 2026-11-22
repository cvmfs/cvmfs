package singularity

import (
	"os"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// File is one entry of the Singularity environment of an image.
type File struct {
	Path string // relative to the image root
	Mode os.FileMode
	// Content is the file content, or the target of a symlink.
	Content string
	// IfMissing entries are only created when the image has nothing at
	// Path; the others replace what the image has.
	IfMissing bool
}

// Files lists the environment that `cvmfs_server overlay -c` injects into
// flat images converted from a registry (cvmfs/swissknife_overlay.cc), so
// that images built from other sources are identical.
func Files(img ocispec.Image) []File {
	dir := func(p string, ifMissing bool) File { return File{p, os.ModeDir | 0755, "", ifMissing} }
	file := func(p, content string) File { return File{p, 0755, content, false} }
	link := func(p, target string) File { return File{p, os.ModeSymlink | 0777, target, true} }
	return []File{
		dir(".singularity.d", false),
		dir(".singularity.d/libs", false),
		dir(".singularity.d/actions", false),
		dir(".singularity.d/env", false),
		dir("dev", true), dir("proc", true), dir("root", true), dir("var", true),
		dir("var/tmp", true), dir("tmp", true), dir("etc", true), dir("sys", true),
		dir("home", true),
		file(".singularity.d/actions/exec", execFileContent),
		file(".singularity.d/actions/run", runFileContent),
		file(".singularity.d/actions/shell", shellFileContent),
		file(".singularity.d/actions/start", startFileContent),
		file(".singularity.d/actions/test", testFileContent),
		file(".singularity.d/env/01-base.sh", baseShFileContent),
		file(".singularity.d/env/90-environment.sh", environmentShFileContent),
		file(".singularity.d/env/91-environment.sh", environmentShFileContent),
		file(".singularity.d/env/95-apps.sh", appsShFileContent),
		file(".singularity.d/env/99-base.sh", base99ShFileContent),
		file(".singularity.d/env/99-runtimevars.sh", base99runtimevarsShFileContent),
		file(".singularity.d/runscript", RunScript(img.Config)),
		file(".singularity.d/startscript", startscriptFileContent),
		file(".singularity.d/env/10-docker2singularity.sh", EnvScript(img.Config)),
		link("singularity", ".singularity.d/runscript"),
		link(".run", ".singularity.d/actions/run"),
		link(".shell", ".singularity.d/actions/shell"),
		link(".exec", ".singularity.d/actions/exec"),
		link(".test", ".singularity.d/actions/test"),
		link("environment", ".singularity.d/env/90-environment.sh"),
	}
}
