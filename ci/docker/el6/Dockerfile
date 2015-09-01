FROM        centos:6
MAINTAINER  Rene Meusel <rene.meusel@cern.ch>

RUN         yum -y update && yum -y install epel-release
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
                                        glibc.i686              \
                                        hardlink                \
                                        httpd                   \
                                        libattr-devel           \
                                        libgcc.i686             \
                                        libstdc++.i686          \
                                        libuuid-devel           \
                                        make                    \
                                        mock                    \
                                        nfs-utils               \
                                        openssl-devel           \
                                        perl-IO-Interface       \
                                        policycoreutils-python  \
                                        python-devel            \
                                        rpm-build               \
                                        screen                  \
                                        selinux-policy-devel    \
                                        selinux-policy-targeted \
                                        sudo                    \
                                        sysvinit-tools          \
                                        tree                    \
                                        zlib-devel

RUN         useradd sftnight && usermod -a -G mock sftnight
USER        sftnight
WORKDIR     /home/sftnight
