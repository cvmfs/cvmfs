TRACE_DIR=/tmp/cvmfs-trace
rm -rf $TRACE_DIR
mkdir $TRACE_DIR
chmod a+rw $TRACE_DIR

echo "*** Writing config file..."

sudo cp /etc/cvmfs/default.local $TRACE_DIR/config.backup
sudo truncate -s0 /etc/cvmfs/default.local

sudo echo "CVMFS_REPOSITORIES=sft.cern.ch" >> /etc/cvmfs/default.local
sudo echo "CVMFS_HTTP_PROXY=http://ca-proxy.cern.ch:3128" >> /etc/cvmfs/default.local
sudo echo "CVMFS_TRACEFILE=$TRACE_DIR/cvmfs-@fqrn@.trace.log" >> /etc/cvmfs/default.local
sudo echo "CVMFS_TRACEBUFFER=16384" >> /etc/cvmfs/default.local
sudo echo "CVMFS_TRACEBUFFER_THRESHOLD=4092" >> /etc/cvmfs/default.local

echo "*** Restarting autofs"

sudo service autofs restart

echo "*** Sleeping for a few seconds..."
sleep 4

echo "*** Starting bash - Please ONLY enter commands that should be traced now..."

bash

echo "*** Bash is done..."
REPOSITORIES=`sudo find /var/lib/cvmfs/shared -maxdepth 1 -name "cvmfs_io*"`
for REPO_PATH in ${REPOSITORIES[@]}
do
  REPO=${REPO_PATH#/var/lib/cvmfs/shared/cvmfs_io.}
  echo "*** Flushing buffer of $REPO"
  sudo cvmfs_talk -i $REPO tracebuffer flush
done
sudo cvmfs_talk tracebuffer flush

echo "*** Writing config file..."
sudo rm /etc/cvmfs/default.local
sudo cp $TRACE_DIR/config.backup /etc/cvmfs/default.local
echo "*** Restarting autofs"
sudo service autofs restart


echo "*** DONE: Tracefiles can be found in $REPO"