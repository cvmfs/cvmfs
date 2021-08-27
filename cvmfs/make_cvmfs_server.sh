#!/bin/sh
#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server

# Create the `cvmfs_server` script

if [ "x$1" != "x" ]; then
    OUTPUT_FILE="$1"
else
    echo "Missing parameter. Call ./make_cvmfs_server.sh <OUTPUT_FILE>"
    exit 1
fi

# Make a fresh OUTPUT_FILE to begin
cat > $OUTPUT_FILE <<!EOF!
#!/bin/sh
#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server


!EOF!

# List the components that make up the resulting file
COMPONENTS="\
    server/cvmfs_server_sys.sh
    server/cvmfs_server_prelude.sh
    server/cvmfs_server_util.sh
    server/cvmfs_server_ssl.sh
    server/cvmfs_server_apache.sh
    server/cvmfs_server_json.sh
    server/cvmfs_server_common.sh
    server/cvmfs_server_health_check.sh
    server/cvmfs_server_compat.sh
    server/cvmfs_server_enter.sh
    server/cvmfs_server_transaction.sh
    server/cvmfs_server_abort.sh
    server/cvmfs_server_publish.sh
    server/cvmfs_server_masterkeycard.sh
    server/cvmfs_server_import.sh
    server/cvmfs_server_mkfs.sh
    server/cvmfs_server_add_replica.sh
    server/cvmfs_server_rmfs.sh
    server/cvmfs_server_resign.sh
    server/cvmfs_server_list_catalogs.sh
    server/cvmfs_server_diff.sh
    server/cvmfs_server_info.sh
    server/cvmfs_server_checkout.sh
    server/cvmfs_server_tag.sh
    server/cvmfs_server_deprecated.sh
    server/cvmfs_server_check.sh
    server/cvmfs_server_list.sh
    server/cvmfs_server_rollback.sh
    server/cvmfs_server_gc.sh
    server/cvmfs_server_snapshot.sh
    server/cvmfs_server_migrate.sh
    server/cvmfs_server_chown.sh
    server/cvmfs_server_eliminate_bulk_hashes.sh
    server/cvmfs_server_eliminate_hardlinks.sh
    server/cvmfs_server_update_info.sh
    server/cvmfs_server_update_repoinfo.sh
    server/cvmfs_server_mount.sh
    server/cvmfs_server_skeleton.sh
    server/cvmfs_server_fix_permissions.sh
    server/cvmfs_server_fix_stats.sh
    server/cvmfs_server_ingest.sh
    server/cvmfs_server_print_stats.sh
    server/cvmfs_server_merge_stats.sh
    server/cvmfs_server_coda.sh
    "

# Includes COMPONENTS in OUTPUT_FILE
for c in $COMPONENTS ; do
    cat $c >> $OUTPUT_FILE
done

# Make OUTPUT_FILE executable
chmod +x $OUTPUT_FILE
