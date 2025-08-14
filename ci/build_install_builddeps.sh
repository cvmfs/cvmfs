
# TODO: sudo handling
# TODO: PWD handling
# TODO arguments: spec file, repo dir, url
# TODO: arguments print
function check_available {
  builtin type -P "$1" &> /dev/null
}


function install_builddeps_from_repo {
  ######## SUSE ###
  if check_available zypper ; then
    if ! check_available rpmbuild ; then
      sudo zypper -n install rpm-build
    fi
    sudo zypper -n install  $(rpmspec --parse $1/packaging/rpm/cvmfs-universal.spec | grep BuildRequires | cut -d' ' -f2 | xargs)
  ######## RHEL-like, fedora ###
  elif check_available yum ; then
    if ! check_available rpmbuild ; then
      sudo yum -y install rpm-build
    fi
    sudo yum builddep -y $1/packaging/rpm/cvmfs-universal.spec
  ######## debian-based  ###
  elif check_available apt-get ; then
    if ! check_available mk-build-deps ; then
      sudo apt-get install -y devscripts equivs
    fi
    mk-build-deps ./packaging/debian/cvmfs/control
    mkdir -p /tmp/cvmfs-build-deps
    mv cvmfs-build-deps_*_all.deb /tmp/cvmfs-build-deps
    sudo apt-get install -y /tmp/cvmfs-build-deps/cvmfs-build-deps_*_all.deb
  fi
}

function list_builddeps {
  #TODO
}

install_builddeps_from_repo ${1:./}

