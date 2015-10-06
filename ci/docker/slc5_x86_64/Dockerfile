FROM        scratch
MAINTAINER  Rene Meusel <rene.meusel@cern.ch>

ADD         slc5_x86_64.tar.gz /
RUN         yum -y update && yum -y install                     \
                                        buildsys-macros         \
                                        cmake                   \
                                        curl-devel              \
                                        fuse-devel              \
                                        gcc                     \
                                        gcc-c++                 \
                                        git                     \
                                        hardlink                \
                                        libattr-devel           \
                                        make                    \
                                        openssl-devel           \
                                        python-devel            \
                                        rpm-build               \
                                        SysVinit                \
                                        selinux-policy-devel    \
                                        selinux-policy-targeted \
                                        which                   \
                                        zlib-devel

RUN         useradd sftnight && usermod -a -G mock sftnight
USER        sftnight
WORKDIR     /home/sftnight