FROM        scratch
MAINTAINER  Rene Meusel <rene.meusel@cern.ch>

ADD         ubuntu1404_i386.tar.gz /
RUN         apt-get -y update && apt-get -y upgrade
RUN         apt-get -y update && apt-get -y install         \
                                              autotools-dev \
                                              cmake         \
                                              debhelper     \
                                              devscripts    \
                                              libattr1-dev  \
                                              libfuse-dev   \
                                              libssl-dev    \
                                              pkg-config    \
                                              python-dev    \
                                              unzip         \
                                              uuid-dev
