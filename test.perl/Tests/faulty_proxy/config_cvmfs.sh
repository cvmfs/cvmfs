echo "CVMFS_REPOSITORIES=127.0.0.1" > /etc/cvmfs/default.local
echo "CVMFS_TIMEOUT=1" >> /etc/cvmfs/default.local
echo "CVMFS_TIMEOUT_DIRECT=1" >> /etc/cvmfs/default.local
echo "CVMFS_QUOTA_LIMIT=8000" >> /etc/cvmfs/default.local
echo "CVMFS_SERVER_URL=http://127.0.0.1:8080/catalogs" > /etc/cvmfs/config.d/127.0.0.1.conf
echo "CVMFS_PUBLIC_KEY=/tmp/cvmfs_master.pub" >> /etc/cvmfs/config.d/127.0.0.1.conf
echo "CVMFS_HTTP_PROXY=\"http://127.0.0.1:3128\"" >> /etc/cvmfs/config.d/127.0.0.1.conf
