echo "CVMFS_REPOSITORIES=mytestrepo.cern.ch" > /etc/cvmfs/default.local
echo "CVMFS_TIMEOUT=10" >> /etc/cvmfs/default.local
echo "CVMFS_TIMEOUT_DIRECT=5" >> /etc/cvmfs/default.local
echo "CVMFS_QUOTA_LIMIT=8000" >> /etc/cvmfs/default.local
echo "CVMFS_DEBUGLOG=/tmp/cvmfsdebug.log" >> /etc/cvmfs/default.local
echo "CVMFS_SERVER_URL=http://mytestrepo.cern.ch:8080/catalogs" > /etc/cvmfs/config.d/mytestrepo.cern.ch.conf
echo "CVMFS_PUBLIC_KEY=/tmp/cvmfs_master.pub" >> /etc/cvmfs/config.d/mytestrepo.cern.ch.conf
echo "CVMFS_HTTP_PROXY=\"http://mytestrepo.cern.ch:3128\"" >> /etc/cvmfs/config.d/mytestrepo.cern.ch.conf
