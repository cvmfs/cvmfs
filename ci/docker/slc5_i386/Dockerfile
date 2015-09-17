FROM        scratch
MAINTAINER  Rene Meusel <rene.meusel@cern.ch>

ADD         slc5_i386.tar.gz /
RUN         yum -y update && yum -y install                     \
                                        autofs                  \
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
                                        httpd                   \
                                        libattr-devel           \
                                        make                    \
                                        mock                    \
                                        nfs-utils               \
                                        openssl-devel           \
                                        perl-IO-Interface       \
                                        python-devel            \
                                        rpm-build               \
                                        screen                  \
                                        SysVinit                \
                                        tree                    \
                                        selinux-policy-devel    \
                                        selinux-policy-targeted \
                                        zlib-devel

RUN         useradd sftnight && usermod -a -G mock sftnight
USER        sftnight
WORKDIR     /home/sftnight