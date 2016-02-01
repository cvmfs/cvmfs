FROM        scratch
MAINTAINER  Rene Meusel <rene.meusel@cern.ch>

ADD         slc5_i386.tar.gz /
RUN         yum -y update && yum -y install                     \
                                        buildsys-macros         \
                                        cmake                   \
                                        curl-devel              \
                                        fuse                    \
                                        fuse-devel              \
                                        gcc                     \
                                        gcc-c++                 \
                                        gdb                     \
                                        git                     \
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
                                        voms-devel              \
                                        which                   \ 
                                        valgrind-devel          \
                                        zlib-devel

RUN         useradd sftnight
USER        sftnight
WORKDIR     /home/sftnight
