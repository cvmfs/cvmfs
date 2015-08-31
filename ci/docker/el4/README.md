# From-scratch CentOS 4.9 Docker image

As-minimal-as-possible CentOS 4.9 image using `yum` and some `chroot` magic.
The idea of checking in a large, opaque binary file makes me itch, but the
Docker model doesn't currently allow for more control over image creation.  This
is hopefully the only time I'll have to do this…

Ok, this also contains the [EPEL](http://fedoraproject.org/wiki/EPEL) repo
configs.  But it's still pretty minimal.

Even so, it's too big to put into GitHub:

    remote: error: GH001: Large files detected.
    remote: error: Trace: 12b8141feda3e55a3296427b879875da
    remote: error: See http://git.io/iEPt8g for more information.
    remote: error: File centos49.tar.xz is 134.86 MB; this exceeds GitHub's file size limit of 100 MB

So, no automated builds.

## generating filesystem image

    docker run --privileged -i -t -v $PWD:/srv centos:centos6 /srv/build.sh

## Credit

The CentOS 4 image creation script was adopted from
[Brian Lador](https://github.com/blalor)'s 
[GitHub repository](https://github.com/blalor/docker-centos4-base).
