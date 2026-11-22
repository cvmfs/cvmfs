package singularity

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func TestEnvScriptQuoting(t *testing.T) {
	cfg := ocispec.ImageConfig{Env: []string{
		"PATH=/opt/a b:/bin", `FOO=a b$c"d\e` + "`x`", "EMPTY", "KEEP=image",
	}}
	script := filepath.Join(t.TempDir(), "env.sh")
	if err := os.WriteFile(script, []byte(EnvScript(cfg)), 0755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("/bin/sh", "-c", `. "$0"; printf '%s|%s|%s|%s' "$PATH" "$FOO" "${EMPTY-unset}" "$KEEP"`, script)
	cmd.Env = []string{"KEEP=user", "PATH=/usr/bin:/bin"}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	want := "/opt/a b:/bin|" + `a b$c"d\e` + "`x`" + "||user"
	if string(out) != want {
		t.Errorf("got %q, want %q", out, want)
	}
}

func TestRunScript(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "runscript")
	cfg := ocispec.ImageConfig{Entrypoint: []string{"/bin/echo", "a b"}, Cmd: []string{"$HOME"}}
	if err := os.WriteFile(script, []byte(RunScript(cfg)), 0755); err != nil {
		t.Fatal(err)
	}
	for args, want := range map[string]string{"": "a b $HOME\n", "x y": "a b x y\n"} {
		out, err := exec.Command(script, strings.Fields(args)...).CombinedOutput()
		if err != nil || string(out) != want {
			t.Errorf("runscript %q: got %q (%v), want %q", args, out, err, want)
		}
	}
}
