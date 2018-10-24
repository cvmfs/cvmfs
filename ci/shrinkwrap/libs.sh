#!/bin/sh

DESTDIR=$1
[ "x$DESTDIR" != x ] || return 1

cp -v /lib64/ld-linux-x86-64.so.2 $DESTDIR/lib64/

libs_missing=1
while [ $libs_missing -eq 1 ]; do
  libs_missing=0
  for f in $(find ${DESTDIR}/bin ${DESTDIR}/lib ${DESTDIR}/usr -type f); do
    libs=
    if ldd $f >/dev/null 2>&1; then
      libs=$(ldd $f | awk '{print $3}' | grep -v 0x | grep -v '^$')
    fi
    [ -z "$libs" ] && continue
    for l in $libs; do
      libname=$(basename $l)
      if [ ! -f ${DESTDIR}/lib/$libname ]; then
        libs_missing=1
        cp -v $l ${DESTDIR}/lib/$libname
      fi
    done
  done
done
