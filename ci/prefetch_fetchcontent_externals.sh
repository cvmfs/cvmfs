#!/bin/sh

#
# Vendor offline build dependencies into a prepared source tree:
# - run 'go mod vendor' for Go components
# - download FetchContent archives into externals/download
#

set -e

if [ $# -lt 2 ]; then
  echo "Usage: $0 <prepared source tree> <download cache directory>"
  exit 1
fi

CVMFS_SOURCE_TREE="$1"
DOWNLOAD_CACHE_DIR="$2"

for godir in ducc gateway snapshotter cvmfs/config; do
  if [ -f "${CVMFS_SOURCE_TREE}/${godir}/go.mod" ]; then
    (cd "${CVMFS_SOURCE_TREE}/${godir}" && go mod vendor)
  fi
done

mkdir -p "${DOWNLOAD_CACHE_DIR}"

manifest_file=$(mktemp)
cmake_file_list=$(mktemp)
find "${CVMFS_SOURCE_TREE}/externals" -mindepth 2 -maxdepth 2 -name CMakeLists.txt -type f | sort > "${cmake_file_list}"

while IFS= read -r file; do
  [ -f "$file" ] || continue
  awk '
    function resolve(str,    prev, varname) {
      prev = ""
      while (str != prev && match(str, /\$\{[A-Za-z0-9_]+\}/)) {
        prev = str
        varname = substr(str, RSTART + 2, RLENGTH - 3)
        gsub("\\$\\{" varname "\\}", vars[varname], str)
      }
      return str
    }
    function extract_quoted_values(line, values,    rest, count, val, mstart, mlen) {
      count = 0
      rest = line
      while (match(rest, /"[^"]*"/)) {
        # Capture the match position before resolve(), which calls match()
        # internally and would otherwise clobber the global RSTART/RLENGTH,
        # leaving "rest" unadvanced and looping forever.
        mstart = RSTART
        mlen = RLENGTH
        val = substr(rest, mstart + 1, mlen - 2)
        count++
        values[count] = resolve(val)
        rest = substr(rest, mstart + mlen)
      }
      return count
    }
    function basename(path,    n, parts) {
      n = split(path, parts, "/")
      return parts[n]
    }
    /^[[:space:]]*set\([A-Za-z0-9_]+[[:space:]]+/ {
      line = $0
      sub(/^[[:space:]]*set\(/, "", line)
      var = line
      sub(/[[:space:]].*$/, "", var)
      n = extract_quoted_values($0, quoted)
      if (n > 0) {
        vals[var] = ""
        for (i = 1; i <= n; ++i) {
          vals[var] = vals[var] quoted[i] "\034"
        }
        # Record single-valued sets (e.g. version strings) as scalars so that
        # resolve() can interpolate them into later ${VAR} references in URLs.
        if (n == 1) {
          vars[var] = quoted[1]
        }
      }
    }
    END {
      for (var in vals) {
        if (var ~ /_URL$/ && var !~ /_URL_MIRROR$/) {
          n = split(vals[var], primary, "\034")
          if (n <= 1 || primary[1] !~ /^https?:\/\//) {
            continue
          }
          printf "%s", basename(primary[1])
          for (i = 1; i < n; ++i) {
            printf "\t%s", primary[i]
          }
          mirror_var = var "_MIRROR"
          if (mirror_var in vals) {
            m = split(vals[mirror_var], mirror, "\034")
            for (i = 1; i < m; ++i) {
              printf "\t%s", mirror[i]
            }
          }
          printf "\n"
        }
      }
    }
  ' "$file" >> "${manifest_file}"
done < "${cmake_file_list}"

if [ -f "${CVMFS_SOURCE_TREE}/cmake/Modules/SetupGoogleTest.cmake" ]; then
  echo "googletest-1.17.0.tar.gz	https://github.com/google/googletest/releases/download/v1.17.0/googletest-1.17.0.tar.gz	https://ecsft.cern.ch/dist/cvmfs/build_externals/googletest-1.17.0.tar.gz" >> "${manifest_file}"
fi

sorted_manifest=$(mktemp)
sort -u "${manifest_file}" > "${sorted_manifest}"

while IFS=$(printf '\t') read -r filename url1 url2 url3 url4; do
  [ -n "$filename" ] || continue
  destination="${DOWNLOAD_CACHE_DIR}/${filename}"
  [ -f "$destination" ] && continue

  for url in "$url1" "$url2" "$url3" "$url4"; do
    [ -n "$url" ] || continue
    echo "$url" | grep -q '\${' && {
      echo "failed to resolve FetchContent URL for $filename: $url"
      rm -f "${manifest_file}" "${cmake_file_list}" "${sorted_manifest}"
      exit 1
    }
    echo "Prefetching $filename from $url"
    if curl -fL --retry 5 --retry-delay 5 \
      --connect-timeout 30 --max-time 600 \
      -o "$destination" \
      "$url" < /dev/null
    then
      break
    fi
    rm -f "$destination"
  done

  [ -f "$destination" ] || {
    echo "failed to download $filename from all configured URLs"
    rm -f "${manifest_file}" "${cmake_file_list}" "${sorted_manifest}"
    exit 1
  }
done < "${sorted_manifest}"

rm -f "${manifest_file}" "${cmake_file_list}" "${sorted_manifest}"
