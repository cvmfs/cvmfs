#!/bin/sh
#
# Add the Debian changelog entry for a cvmfs release. Run it before tagging and
# commit the result; ci/cvmfs/deb.sh takes the revision and date from it.
#
# Usage: ci/deb_changelog_release.sh [--skip-prerelease] <version> [<revision>]
#   version            upstream version the tag resolves to, e.g. 2.15.0 or
#                      2.15.0~pre7
#   revision           Debian revision (default: 1)
#   --skip-prerelease  leave the changelog unchanged for ~pre versions, so the
#                      UNRELEASED entry is kept for the final release
#
# An UNRELEASED top entry is finalised with its bullets kept; otherwise a
# "New upstream release" entry is added. DEBFULLNAME / DEBEMAIL override the
# maintainer, which defaults to the git user.
#

set -e

skip_prerelease=0
if [ "$1" = "--skip-prerelease" ]; then
  skip_prerelease=1
  shift
fi
version=$1
revision=${2:-1}
if [ -z "$version" ]; then
  sed -n '3,15p' "$0" | sed 's/^# \{0,1\}//'
  exit 1
fi
echo "$version" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+(~[0-9A-Za-z.]+)?$' \
  || { echo "invalid version: $version" >&2; exit 1; }
case "$version" in
  *~pre*)
    if [ "$skip_prerelease" = 1 ]; then
      echo "$version is a pre-release, leaving the changelog unchanged"
      exit 0
    fi ;;
esac

repo=$(cd "$(dirname "$0")/.."; pwd)
changelog="$repo/packaging/debian/cvmfs/changelog"

header="cvmfs (${version}-${revision}) unstable; urgency=medium"
if head -n 1 "$changelog" | grep -qF "cvmfs (${version}-${revision}) unstable;"; then
  echo "$changelog already has ${version}-${revision}" >&2
  exit 1
fi

name=${DEBFULLNAME:-$(git -C "$repo" config user.name)}
email=${DEBEMAIL:-$(git -C "$repo" config user.email)}
trailer=" -- $name <$email>  $(LC_ALL=C date '+%a, %d %b %Y %H:%M:%S %z')"

tmp="$changelog.tmp"
if head -n 1 "$changelog" | grep -q ') UNRELEASED;'; then
  awk -v header="$header" -v trailer="$trailer" '
    NR == 1                { print header; next }
    !done && /^ -- /       { print trailer; done = 1; next }
                           { print }' "$changelog" > "$tmp"
else
  {
    printf '%s\n\n  * New upstream release, see ChangeLog\n\n%s\n\n' \
      "$header" "$trailer"
    cat "$changelog"
  } > "$tmp"
fi
mv "$tmp" "$changelog"

sed -n '1,/^ -- /p' "$changelog"
