#!/bin/bash
CDIR=$(pwd)
BASEDIR=$(dirname $0)
PMDOC_TEMPLATE=$BASEDIR/cvmfs.pmdoc.template
PKG_DOC=$BASEDIR/cvmfs.pmdoc

function usage()
{
  echo -e "Script to generate a Package for software distribution on Mac OS X.
           Options:
           -b <build directory> (mandatory)
           -h print this help\n" 1>&2
}

function cleanup_install()
{
  echo "Cleaning installation remnants in $TMPDIR ..."
  rm -rf $TMPDIR
  echo "Cleaning package temporary package description $PKG_DOC ..."
  rm -rf $PKG_DOC
}

function check_pkgmaker()
{
  echo "Looking for PackageMaker ..."
  PKGMAKER=$(which packagemaker)
  if [ $? -ne 0 ]; then
    PKGMAKER=/Applications/PackageMaker.app/Contents/MacOS/PackageMaker
    if [ ! -x $PKGMAKER ]; then
      echo "PackageMaker not found/executable in PATH or $PKGMAKER"
      cleanup_install
      exit 2
    fi
  fi
  echo "PackageMaker found in $PKGMAKER"
}

function create_pmdoc()
{
  echo "Creating pmdoc using template $PMDOC_TEMPLATE ..."
  cp -a $PMDOC_TEMPLATE $PKG_DOC
  sed -i.bak -e "s/@PackageOutput@/$(echo $CDIR/cvmfs.pkg | sed -e "s,/,\\\\/,g")/g" $PKG_DOC/*.xml
  sed -i.bak -e "s/@PackageInput@/$(echo $TMPDIR | sed -e "s,/,\\\\/,g")/g" $PKG_DOC/*.xml
  rm -f $PKG_DOC/*.bak
}

function install_cvmfs()
{
  echo "Installing cvmfs to $TMPDIR ..."
  cd $BUILDDIR
  make install DESTDIR=$TMPDIR
  if [ $? -ne 0 ]; then
    echo "Installation to $TMPDIR failed."
    cleanup_install
    exit 3
  fi
  cd $CDIR
}

function build_pkg()
{
  echo "Creating package $CDIR/cvmfs.pkg ..."
  $PKGMAKER --doc $PKG_DOC --verbose --root-volume-only --out $CDIR/cvmfs.pkg 
  if [ $? -ne 0 ]; then
    echo "Package creation failed!"
    cleanup_install
    exit 4
  fi
}

function prepare_install()
{
  echo "Creating temporary installation directory ..."
  TMPDIR=/tmp/cvmfs_pkg/CVMFS_Package; mkdir -p $TMPDIR #$(mktemp -d -t /tmp)
  if [ $? -ne 0 ]; then
    echo "Creation of temporary installtion directory failed!"
    exit 5
  fi
}

while [ $# -ge 1 ]; do
  case $1 in
    -b ) BUILDDIR=$2; shift 2 ;;
    -h ) usage 1>&2; exit 1 ;;
    -* ) echo "$0: unrecognised option $1, use -h for help" 1>&2; exit 1 ;;
    *  ) echo "$0: unrecognised parameter $1, use -h for help" 1>&2; exit 1 ;;
  esac
done

if [ x"$BUILDDIR" == "x" ]; then
  echo "$0: not enough information provided, use -h for help" 1>&2
  exit 1
fi

### create temporary directory for the installation
prepare_install

### find MacOSX PackageMaker
check_pkgmaker

install_cvmfs

### create packagemaker doc from template
create_pmdoc

build_pkg

cleanup_install