# CernVM-FS in Docker Containers

There are two options to mount CernVM-FS in docker containers.  The first option is to bind mount a mounted repository as a volume into the container.  This has the advantage that the cvmfs cache is shared among multiple containers.  The second option is to mount a repository inside a container, which requires a _privileged_ container.

In both cases, autofs must not be used. Docker has currently issues with autofs.

##  Bind mount from the host

In order to bind mount a repository from the host, turn off autofs on the host and mount the repository manually, like:

    service autofs stop  # systemd: systemctl stop autofs
    chkconfig autofs off  # systemd: systemctl disable autofs
    mkdir -p /cvmfs/sft.cern.ch
    mount -t cvmfs sft.cern.ch /cvmfs/sft.cern.ch

Start the docker container with the `-v` option to mount the cvmfs repository inside, like

    docker run -i -t -v /cvmfs/sft.cern.ch:/cvmfs/sft.cern.ch centos /bin/bash

The `-v` option can be used multiple times with different repositories.

## Mount inside a container

In order to use `mount` inside a container, the container must be started in privileged mode, like

    docker run --privileged -i -t centos /bin/bash

In such a container, CernVM-FS can be installed and used the usual way provided that autofs is turned off.
