echo "CVMFS_REPOSITORIES=mytestrepo.cern.ch" > /etc/cvmfs/default.local
echo "CVMFS_TIMEOUT_DIRECT=5" >> /etc/cvmfs/default.local
echo "CVMFS_QUOTA_LIMIT=8000" >> /etc/cvmfs/default.local
echo "CVMFS_SERVER_URL=http://[::1]:8080/catalogs" > /etc/cvmfs/config.d/mytestrepo.cern.ch.conf
echo "CVMFS_PUBLIC_KEY=/tmp/cvmfs_master.pub" >> /etc/cvmfs/config.d/mytestrepo.cern.ch.conf
