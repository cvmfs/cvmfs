#!/bin/sh
#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server

# Create the `cvmfs_server` script


OUTPUT_FILE="cvmfs_server"

# Make an fresh OUTPUT_FILE to begin
echo '
#!/bin/sh
#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server


' > $OUTPUT_FILE

# List the components that make up the resulting file
COMPONENTS="\
    server/cvmfs_server_prelude.sh
    server/cvmfs_server_util.sh
    server/cvmfs_server_ssl.sh
    server/cvmfs_server_apache.sh
    server/cvmfs_server_json.sh
    server/cvmfs_server_common.sh
    server/cvmfs_server_health_check.sh
    server/cvmfs_server_compat.sh
    server/cvmfs_server_transaction.sh
    server/cvmfs_server_abort.sh
    server/cvmfs_server_publish.sh
    server/cvmfs_server_mkfs.sh
    server/cvmfs_server_coda.sh
    "

# Includes COMPONENTS in OUTPUT_FILE
for c in $COMPONENTS ; do
    cat $c >> $OUTPUT_FILE
done

# Make OUTPUT_FILE executable
chmod +x $OUTPUT_FILE
