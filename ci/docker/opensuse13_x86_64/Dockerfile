## See README.md, build.sh and ../build_on_docker.sh

FROM       scratch
MAINTAINER Rene Meusel <rene.meusel@cern.ch>

ADD        opensuse13_x86_64.tar.gz /
RUN        zypper -n update && zypper -n install          \
                                  cmake                   \
                                  fuse-devel              \
                                  gcc                     \
                                  gcc-c++                 \
                                  gdb                     \
                                  git                     \
                                  libattr-devel           \
                                  libcap-devel            \
                                  libcurl-devel           \
                                  libopenssl-devel        \
                                  libuuid-devel           \
                                  make                    \
                                  policycoreutils-python  \
                                  python-devel            \
                                  rpm-build               \
                                  selinux-policy-devel    \
                                  selinux-policy-targeted \
                                  unzip                   \
                                  valgrind-devel          \
                                  zlib-devel

RUN        useradd sftnight
USER       sftnight
WORKDIR    /home/sftnight
