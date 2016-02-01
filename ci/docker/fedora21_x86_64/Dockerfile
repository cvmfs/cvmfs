FROM        scratch
MAINTAINER  Rene Meusel <rene.meusel@cern.ch>

ADD         fedora21_x86_64.tar.gz /
RUN         yum -y update && yum -y install                     \
                                        cmake                   \
                                        curl-devel              \
                                        fuse-devel              \
                                        gcc                     \
                                        gcc-c++                 \
                                        git                     \
                                        hardlink                \
                                        libattr-devel           \
                                        libcap-devel            \
                                        libuuid-devel           \
                                        make                    \
                                        openssl-devel           \
                                        policycoreutils-python  \
                                        python-devel            \
                                        rpm-build               \
                                        selinux-policy-devel    \
                                        selinux-policy-targeted \
                                        sysvinit-tools          \
                                        voms-devel              \
                                        which                   \
                                        valgrind-devel          \
                                        zlib-devel

RUN         useradd sftnight
USER        sftnight
WORKDIR     /home/sftnight
