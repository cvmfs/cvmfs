# Versioning manual

CernVM-FS has one base version in the top-level `VERSION` file. The
`cmake/get_version.sh` resolver combines it with Git metadata for development
builds. CMake, the packaging scripts, and source-tarball creation all use this
resolver.

## Inspect the version

From the repository root:

```bash
./cmake/get_version.sh .
```

An untagged checkout produces a version such as:

```text
2.15.0~dev1.0.20260731145317.gitf8d9cd7e63
```

The fields after `dev` are the order, sequence, and UTC commit timestamp. The
final `git` field contains the short Git hash. A tracked modification adds the
suffix `.dirty`. An exact release tag produces `2.15.0`; an exact tag named
`cvmfs-2.15.0-pre1` produces `2.15.0~pre1`. A modified tagged checkout is
reported as a dirty development version instead of a release.

After building, verify the version reported by the client with:

```bash
cvmfs2 --version
```

Shared-library filenames continue to use only the numeric base version, for
example `libcvmfs_client.so.2.15.0`.

## Start a new release series

Use the bump script to change the minor and patch components:

```bash
./ci/bump_version.sh 16 0
```

This changes `VERSION` to `2.16.0` and updates the libcvmfs API minor. The major
version is intentionally retained. For a major-version change, edit `VERSION`
and review the libcvmfs API version separately.

Prerelease state is not stored in `VERSION`. It comes from a Git tag or an
explicit override.

## Tag a prerelease or release

Create tags using the existing `cvmfs-` naming convention:

```bash
# Prerelease: resolves to 2.15.0~pre1
git tag -a cvmfs-2.15.0-pre1 -m "CernVM-FS 2.15.0 pre1"

# Final release: resolves to 2.15.0
git tag -a cvmfs-2.15.0 -m "CernVM-FS 2.15.0"
```

Build packages from the tagged commit so that `HEAD` exactly matches the tag.

## Override a version

Override the complete resolved version through the environment:

```bash
CVMFS_VERSION_OVERRIDE=2.15.0~test1 ./cmake/get_version.sh .
CVMFS_VERSION_OVERRIDE=2.15.0~test1 cmake -S . -B build
```

The equivalent CMake option is:

```bash
cmake -S . -B build -DCVMFS_VERSION_OVERRIDE=2.15.0~test1
```

The override must start with a three-component numeric version and use a
package-safe suffix.

## Control development-package ordering

Normal development builds use order `1`. Order `0` is reserved for packages
that must sort below the normal development stream:

```bash
CVMFS_VERSION_ORDER=0 ./cmake/get_version.sh .
```

Use a centrally assigned sequence for published snapshots:

```bash
CVMFS_VERSION_SEQUENCE=1842 ./cmake/get_version.sh .
```

Both can also be passed as CMake options:

```bash
cmake -S . -B build \
  -DCVMFS_VERSION_ORDER=1 \
  -DCVMFS_VERSION_SEQUENCE=1842
```

The timestamp normally comes from the Git commit and is reproducible. Override
it only when necessary:

```bash
CVMFS_VERSION_TIMESTAMP=20260801090000 ./cmake/get_version.sh .
```

Order is compared before sequence and timestamp. Increasing the order to `2`
places a package above every future order-`1` package, so higher order lanes
should be used sparingly.

Use the RPM `Release` or Debian package revision to rebuild the same source.
Change the version sequence only when publishing a new source snapshot.

## Build packages

The packaging scripts resolve and embed the version automatically:

```bash
./ci/cvmfs/rpm.sh "$PWD" /tmp/cvmfs-rpm
./ci/cvmfs/deb.sh "$PWD" /tmp/cvmfs-deb
```

Pass ordering or override variables in the environment:

```bash
CVMFS_VERSION_SEQUENCE=1842 \
  ./ci/cvmfs/rpm.sh "$PWD" /tmp/cvmfs-rpm
```

The optional legacy nightly-build argument is still accepted and is treated as
the development-version sequence. The RPM spec's `Version` comes from the
`cvmfs_version` RPM macro; the packaging scripts pass it with `rpmbuild
--define "cvmfs_version ..."` rather than editing the spec file. Without that
define — e.g. when parsing the spec directly with `dnf builddep` — it falls
back to the plain `VERSION` file, staged as `Source3` next to the spec.

## Build a source tarball

```bash
./ci/build_cvmfs_sourcetarball.sh "$PWD" /tmp/cvmfs-dist
```

The archive contains a `CVMFS_VERSION` file with the fully resolved version.
Consequently, binaries and packages built from the archive retain the same
version without requiring Git.

Version resolution follows this precedence:

1. `CVMFS_VERSION_OVERRIDE`
2. `CVMFS_VERSION` embedded in a source archive
3. an exact `cvmfs-*` Git tag
4. an automatically generated Git development version
5. the numeric value in `VERSION`
