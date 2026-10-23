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

# Print the <algorithm> digest of a file as lowercase hex.
file_digest() {
  digest_algorithm="$1"
  digest_file="$2"

  if command -v "${digest_algorithm}sum" > /dev/null 2>&1; then
    "${digest_algorithm}sum" "${digest_file}" | cut -d' ' -f1
  elif [ "${digest_algorithm}" = "md5" ] && command -v md5 > /dev/null 2>&1; then
    md5 -q "${digest_file}"
  elif [ "${digest_algorithm}" != "md5" ] && command -v shasum > /dev/null 2>&1; then
    shasum -a "${digest_algorithm#sha}" "${digest_file}" | cut -d' ' -f1
  elif command -v openssl > /dev/null 2>&1; then
    openssl dgst "-${digest_algorithm}" "${digest_file}" | sed 's/^.*= *//'
  else
    return 1
  fi
}

# Check a file against a "<ALGORITHM>=<hex digest>" string, i.e. the value of the
# URL_HASH of the corresponding FetchContent_Declare().
verify_digest() {
  verify_file="$1"
  expected_algorithm=$(echo "$2" | cut -d= -f1 | tr 'A-Z' 'a-z')
  expected_digest=$(echo "$2" | cut -d= -f2- | tr 'A-Z' 'a-z')

  case "${expected_algorithm}" in
    md5|sha1|sha224|sha256|sha384|sha512) ;;
    *)
      echo "unsupported URL_HASH algorithm '$2' for ${verify_file}" >&2
      exit 1
      ;;
  esac

  actual_digest=$(file_digest "${expected_algorithm}" "${verify_file}") || {
    echo "cannot compute the ${expected_algorithm} digest of ${verify_file}" >&2
    exit 1
  }

  [ "${actual_digest}" = "${expected_digest}" ]
}

for godir in ducc gateway snapshotter; do
  if [ -f "${CVMFS_SOURCE_TREE}/${godir}/go.mod" ]; then
    (cd "${CVMFS_SOURCE_TREE}/${godir}" && go mod vendor)
  fi
done

mkdir -p "${DOWNLOAD_CACHE_DIR}"

manifest_file=$(mktemp)
cmake_file_list=$(mktemp)
sorted_manifest=$(mktemp)
trap 'rm -f "${manifest_file}" "${cmake_file_list}" "${sorted_manifest}"' EXIT

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
          hash_var = var
          sub(/_URL$/, "_HASH", hash_var)
          # "-" and not an empty field: read splits on runs of tabs, so an empty
          # field would shift the URLs into the digest column.
          hash = (hash_var in vars) ? vars[hash_var] : "-"
          printf "%s\t%s", basename(primary[1]), hash
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
  echo "googletest-1.17.0.tar.gz	MD5=b6f100bc2a5853a48046aa168ececf84	https://github.com/google/googletest/releases/download/v1.17.0/googletest-1.17.0.tar.gz	https://ecsft.cern.ch/dist/cvmfs/build_externals/googletest-1.17.0.tar.gz" >> "${manifest_file}"
fi

sort -u "${manifest_file}" > "${sorted_manifest}"

while IFS=$(printf '\t') read -r filename hash url1 url2 url3 url4; do
  [ -n "$filename" ] || continue

  # Without a digest we cannot tell an archive from an error page, and the
  # offline build would happily use whatever ends up in the cache.
  if [ -z "$hash" ] || [ "$hash" = "-" ]; then
    echo "no URL_HASH declared for $filename, refusing to prefetch it unverified" >&2
    exit 1
  fi

  destination="${DOWNLOAD_CACHE_DIR}/${filename}"
  if [ -f "$destination" ]; then
    verify_digest "$destination" "$hash" && continue
    echo "cached $filename does not match $hash, downloading it again" >&2
    rm -f "$destination"
  fi

  for url in "$url1" "$url2" "$url3" "$url4"; do
    [ -n "$url" ] || continue
    echo "$url" | grep -q '\${' && {
      echo "failed to resolve FetchContent URL for $filename: $url" >&2
      exit 1
    }
    echo "Prefetching $filename from $url"
    if curl -fL --retry 5 --retry-delay 5 \
      --connect-timeout 30 --max-time 600 \
      -o "$destination" \
      "$url" < /dev/null
    then
      verify_digest "$destination" "$hash" && break
      # Servers do answer with an HTTP 200 error page or anti-bot interstitial
      # every now and then; that is a failed download, not a valid archive.
      echo "$url did not return $hash for $filename, trying the next URL" >&2
    fi
    rm -f "$destination"
  done

  [ -f "$destination" ] || {
    echo "failed to download $filename from all configured URLs" >&2
    exit 1
  }
done < "${sorted_manifest}"
