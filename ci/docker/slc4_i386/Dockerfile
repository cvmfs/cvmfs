## See README.md, build.sh and ../build_on_docker.sh

FROM       scratch
MAINTAINER Rene Meusel <rene.meusel@cern.ch>

ADD        slc4_i386.tar.gz /

# update available yum repositories and re-route them to our private clone
RUN        rm -f ${instroot}/etc/yum.repos.d/sl-rhaps.repo      \
                 ${instroot}/etc/yum.repos.d/sl-testing.repo    \
                 ${instroot}/etc/yum.repos.d/sl4x-contrib.repo  \
                 ${instroot}/etc/yum.repos.d/sl4x-errata.repo   \
                 ${instroot}/etc/yum.repos.d/dag.repo           \
                 ${instroot}/etc/yum.repos.d/dries.repo         \
                 ${instroot}/etc/yum.repos.d/atrpms.repo        \
                 ${instroot}/etc/yum.repos.d/sl4x-fastbugs.repo \
                 ${instroot}/etc/yum.repos.d/sl4x.repo
ADD        sl4x.repo          /etc/yum.repos.d/
ADD        sl4x-epel.repo     /etc/yum.repos.d/
ADD        sl4x-fastbugs.repo /etc/yum.repos.d/

RUN        yum -y update && yum -y install                      \
                                        bash                    \
                                        checkpolicy             \
                                        chkconfig               \
                                        cmake                   \
                                        coreutils               \
                                        e2fsprogs               \
                                        e2fsprogs-devel         \
                                        fuse-devel              \
                                        fuse-libs               \
                                        gawk                    \
                                        gcc4                    \
                                        gcc4-c++                \
                                        gdb                     \
                                        git                     \
                                        glibc-common            \
                                        grep                    \
                                        gzip                    \
                                        initscripts             \
                                        libattr-devel           \
                                        libcap-devel            \
                                        make                    \
                                        openssl                 \
                                        openssl-devel           \
                                        patch                   \
                                        perl                    \
                                        pkgconfig               \
                                        python-devel            \
                                        rpm-build               \
                                        sed                     \
                                        selinux-policy-targeted \
                                        shadow-utils            \
                                        SysVinit                \
                                        unzip                   \
                                        voms-devel              \
                                        which                   \
                                        zlib

# fix bogus %{_sysconfdir} and %{_defaultdocdir} RPM macro definitions
RUN        sed -i -e 's/^\(%_sysconfdir[[:space:]]\+\).*$/\1\/etc/g'                  /usr/lib/rpm/macros && \
           sed -i -e 's/^\(%_defaultdocdir[[:space:]]\+\).*$/\1%{_usr}\/share\/doc/g' /usr/lib/rpm/macros
