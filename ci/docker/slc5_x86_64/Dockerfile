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
                                        gdb                     \
                                        git                     \
                                        gridsite                \
                                        hardlink                \
                                        libattr-devel           \
                                        libcap-devel            \
                                        make                    \
                                        openssl-devel           \
                                        python-devel            \
                                        rpm-build               \
                                        SysVinit                \
                                        selinux-policy-devel    \
                                        selinux-policy-targeted \
                                        which                   \
                                        voms-devel              \
                                        valgrind-devel          \
                                        zlib-devel

RUN         useradd sftnight
USER        sftnight
WORKDIR     /home/sftnight
