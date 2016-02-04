FROM        scratch
MAINTAINER  Rene Meusel <rene.meusel@cern.ch>

ADD         slc6_i386.tar.gz /
RUN         yum -y update && yum -y install                         \
                                        cmake                       \
                                        curl-devel                  \
                                        fuse-devel                  \
                                        gcc                         \
                                        gcc-c++                     \
                                        git                         \
                                        globus-common-devel         \
                                        globus-gsi-callback-devel   \
                                        globus-gsi-cert-utils-devel \
                                        globus-gsi-credential-devel \
                                        globus-gsi-sysconfig-devel  \
                                        hardlink                    \
                                        libattr-devel               \
                                        libcap-devel                \
                                        libuuid-devel               \
                                        make                        \
                                        openssl-devel               \
                                        policycoreutils-python      \
                                        python-devel                \
                                        rpm-build                   \
                                        selinux-policy-devel        \
                                        selinux-policy-targeted     \
                                        sysvinit-tools              \
                                        valgrind-devel              \
                                        voms-devel                  \
                                        which                       \
                                        zlib-devel

RUN         useradd sftnight
USER        sftnight
WORKDIR     /home/sftnight
