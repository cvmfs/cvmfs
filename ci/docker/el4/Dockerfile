## See README.md, build.sh and ../build_on_docker.sh

FROM       cvmfs/slc49_i386
MAINTAINER Rene Meusel <rene.meusel@cern.ch>

RUN        yum -y update && yum -y install                      \
                                        attr                    \
                                        autofs                  \
                                        bash                    \
                                        checkpolicy             \
                                        chkconfig               \
                                        cmake                   \
                                        coreutils               \
                                        curl                    \
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
                                        httpd                   \
                                        initscripts             \
                                        libattr-devel           \
                                        make                    \
                                        openssl                 \
                                        openssl-devel           \
                                        patch                   \
                                        perl                    \
                                        pkgconfig               \
                                        psmisc                  \
                                        python-devel            \
                                        rpm-build               \
                                        sed                     \
                                        selinux-policy-targeted \
                                        shadow-utils            \
                                        sudo                    \
                                        SysVinit                \
                                        unzip                   \
                                        which                   \
                                        zlib

# fix bogus %{_sysconfdir} and %{_defaultdocdir} RPM macro definitions
RUN        sed -i -e 's/^\(%_sysconfdir[[:space:]]\+\).*$/\1\/etc/g'                  /usr/lib/rpm/macros && \
           sed -i -e 's/^\(%_defaultdocdir[[:space:]]\+\).*$/\1%{_usr}\/share\/doc/g' /usr/lib/rpm/macros
