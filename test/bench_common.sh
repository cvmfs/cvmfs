function die() {
  echo error: "$@" >&2
  exit 1
}

function runall() {
  # runall fn
  for TEST in 001-atlas 003-alice 004-cms 005-cmsmulti; do
    for KCACHE in kcache nokcache; do
      for CACHE in posix ram_malloc ram_arena; do
        for BACKEND in fuse parrot; do
          $1
          rc=$?
          if [ $? -ne 0 ]; then
            echo "error ($rc) $TEST/$KCACHE/$CACHE/$BACKEND"
          fi
        done
      done
    done
  done
}
