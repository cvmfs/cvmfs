#!/bin/sh

if [ ! -f ~arch ]; then
  echo '#include <stdio.h>
    int main(){printf("%d", sizeof(long));return 0;}' | gcc -x c -
  SIZEOF_LONG=$(./a.out)
  rm -f a.out
  case $SIZEOF_LONG in
    8)
      echo 64opt > ~arch
    ;;
    4)
      echo 32BI > ~arch
    ;;
  esac
fi

make clean
make ARCH=$(cat ~arch)
strip -S libsha3.a

