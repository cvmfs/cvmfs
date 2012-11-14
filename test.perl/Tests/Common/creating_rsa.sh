openssl genrsa -out /tmp/cvmfs_test.key 2048
openssl req -new -subj "/C=CH/ST=n\/a/L=Geneva/O=CERN/OU=PH-SFT/CN=CVMFS Test Certificate" -key /tmp/cvmfs_test.key -out /tmp/cvmfs_test.csr
openssl x509 -req -days 365 -in /tmp/cvmfs_test.csr -signkey /tmp/cvmfs_test.key -out /tmp/cvmfs_test.crt 
