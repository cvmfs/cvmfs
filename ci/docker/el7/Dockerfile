FROM        centos:7
MAINTAINER  Rene Meusel <rene.meusel@cern.ch>

RUN         yum -y update && yum -y install epel-release
RUN         yum -y update && yum -y install                     \
                                        attr                    \
                                        autofs                  \
                                        cmake                   \
                                        curl-devel              \
                                        doxygen                 \
                                        fuse                    \
                                        fuse-devel              \
                                        gcc                     \
                                        gcc-c++                 \
                                        gdb                     \
                                        git                     \
                                        glibc.i686              \
                                        graphviz                \
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
                                        policycoreutils-python  \
                                        psmisc                  \
                                        python-devel            \
                                        rpm-build               \
                                        screen                  \
                                        selinux-policy-devel    \
                                        selinux-policy-targeted \
                                        sysvinit-tools          \
                                        tree                    \
                                        zlib-devel

RUN         useradd sftnight && usermod -a -G mock sftnight
USER        sftnight
WORKDIR     /home/sftnight
