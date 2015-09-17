FROM        scratch
MAINTAINER  Rene Meusel <rene.meusel@cern.ch>

ADD         fedora21_i386.tar.gz /
RUN         yum -y update && yum -y install                     \
                                        autofs                  \
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
                                        libuuid-devel           \
                                        make                    \
                                        nfs-utils               \
                                        openssl-devel           \
                                        perl-IO-Interface       \
                                        policycoreutils-python  \
                                        python-devel            \
                                        rpm-build               \
                                        selinux-policy-devel    \
                                        selinux-policy-targeted \
                                        sudo                    \
                                        sysvinit-tools          \
                                        tree                    \
                                        zlib-devel

RUN         useradd sftnight
USER        sftnight
WORKDIR     /home/sftnight
