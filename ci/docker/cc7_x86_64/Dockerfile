FROM        scratch
MAINTAINER  Rene Meusel <rene.meusel@cern.ch>

ADD         cc7_x86_64.tar.gz /
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
                                        libuuid-devel           \
                                        make                    \
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

RUN         useradd sftnight
USER        sftnight
WORKDIR     /home/sftnight
