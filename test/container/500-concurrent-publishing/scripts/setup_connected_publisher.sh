#!/bin/bash
#jsudo yum install -y https://cvmrepo.s3.cern.ch/cvmrepo/yum/cvmfs-release-latest.noarch.rpm
#sudo yum install -y cvmfs cvmfs-server
FQRN=test.repo.org
CVMFS_GATEWAY_URL=http://cvmfs-gw1
CVMFS_SERVER_DEBUG=3 cvmfs_server mkfs -w $CVMFS_GATEWAY_URL/cvmfs/$FQRN \
                         -u gw,/srv/cvmfs/$FQRN/data/txn,$CVMFS_GATEWAY_URL:4929/api/v1 \
                         -k /etc/cvmfs/keys -o `whoami` $FQRN
cvmfs_server transaction
cvmfs_server publish

