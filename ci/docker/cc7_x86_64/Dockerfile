FROM        scratch
MAINTAINER  Rene Meusel <rene.meusel@cern.ch>

ADD         cc7_x86_64.tar.gz /
RUN         yum -y update && yum -y install epel-release
RUN         yum -y install                     \
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
                       which                   \
                       valgrind-devel          \
                       voms-devel              \
                       zlib-devel

RUN         useradd sftnight
USER        sftnight
WORKDIR     /home/sftnight
