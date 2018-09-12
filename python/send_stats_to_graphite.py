#!/usr/bin/python

import argparse
import datetime
import os
import pickle
import re
import socket
import struct
import sqlite3
import sys
import time
from calendar import timegm


def get_data_publish_stats(last_timestamp, db_file):
    conn = sqlite3.connect(db_file)
    c = conn.cursor()

    # order DESC by id ---> first element of the list has the last start_time
    c.execute('SELECT start_time, files_added, files_removed, files_changed, \
                    duplicated_files, directories_added, directories_removed, \
                    directories_changed, sz_bytes_added, sz_bytes_removed, \
                    sz_bytes_uploaded \
                FROM publish_statistics \
                WHERE start_time > ("%s") \
                ORDER BY publish_id DESC' % last_timestamp)
    return c.fetchall()


def get_data_gc_stats(last_timestamp, db_file):
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute('SELECT start_time, n_preserved_catalogs, n_condemned_catalogs,\
                    n_condemned_objects, sz_condemned_bytes \
                FROM gc_statistics \
                WHERE start_time > ("%s") \
                ORDER BY gc_id DESC' % last_timestamp)
    return c.fetchall()


def run(sock, db_file):
    tuples = ([])
    lines = []

    # Try to open the last_timestamp_sent file, otherwise create it
    # and write in it the current UTC timestamp
    try:
        f = open(os.path.dirname(db_file) + "/last_timestamp_sent", "r+")
        publish_timestamp = f.readline()[:-1]  # delete the newline character
        gc_timestamp = f.readline()
        f.truncate()  # delete file content
        f.seek(0)     # move file cursor
    except IOError:
        # create the file and get the current UTC timestamp
        f = open(os.path.dirname(db_file) + "/last_timestamp_sent", "w+")
        publish_timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        gc_timestamp = publish_timestamp

    publish_stats = get_data_publish_stats(publish_timestamp, db_file)
    gc_stats = get_data_gc_stats(gc_timestamp, db_file)

    if len(publish_stats) > 0:
        publish_timestamp = publish_stats[0][0]
        for x in xrange(0, len(publish_stats)):
            time_obj = time.strptime(publish_stats[x][0], "%Y-%m-%d %H:%M:%S")
            timestamp_epoch = timegm(time_obj)
            tuples.append(('cvmfs.publish.files_added', (timestamp_epoch, publish_stats[x][1])))
            tuples.append(('cvmfs.publish.files_removed', (timestamp_epoch, publish_stats[x][2])))
            tuples.append(('cvmfs.publish.files_changed', (timestamp_epoch, publish_stats[x][3])))
            tuples.append(('cvmfs.publish.duplicated_files', (timestamp_epoch, publish_stats[x][4])))
            tuples.append(('cvmfs.publish.directories_added', (timestamp_epoch, publish_stats[x][5])))
            tuples.append(('cvmfs.publish.directories_removed', (timestamp_epoch, publish_stats[x][6])))
            tuples.append(('cvmfs.publish.directories_changed', (timestamp_epoch, publish_stats[x][7])))
            tuples.append(('cvmfs.publish.sz_bytes_added', (timestamp_epoch, publish_stats[x][8])))
            tuples.append(('cvmfs.publish.sz_bytes_removed', (timestamp_epoch, publish_stats[x][9])))
            tuples.append(('cvmfs.publish.sz_bytes_uploaded', (timestamp_epoch, publish_stats[x][10])))

    if len(gc_stats) > 0:
        gc_timestamp = gc_stats[0][0]
        for x in xrange(0, len(gc_stats)):
            time_obj = time.strptime(gc_stats[x][0], "%Y-%m-%d %H:%M:%S")
            timestamp_epoch = timegm(time_obj)
            tuples.append(('cvmfs.gc.n_preserved_catalogs', (timestamp_epoch, gc_stats[x][1])))
            tuples.append(('cvmfs.gc.n_condemned_catalogs', (timestamp_epoch, gc_stats[x][2])))
            tuples.append(('cvmfs.gc.n_condemned_objects', (timestamp_epoch, gc_stats[x][3])))
            tuples.append(('cvmfs.gc.sz_condemned_bytes', (timestamp_epoch, gc_stats[x][4])))

    # build the package
    package = pickle.dumps(tuples, 1)
    size = struct.pack('!L', len(package))
    sock.sendall(size)
    sock.sendall(package)

    # write the last start_time into the last_timestamp_sent file
    f.write(publish_timestamp + "\n")  # write last publish start_time
    f.write(gc_timestamp)              # write last gc start_time
    f.close()


def main():
    parser = argparse.ArgumentParser(description='Send stats to carbon server using pickle.\
                                                 If the `last_timestamp_sent` file does not exist \
                                                 in the same directory as <db_file>, this script \
                                                 will create one and it will write the current UTC timestamp in the file.')
    parser.add_argument('db_file', metavar='<db_file>', type=str,
                        help='SQLite database file path')
    parser.add_argument('CARBON_SERVER_IP', metavar='<IP>', type=str,
                        help='carbon server ip')
    parser.add_argument('CARBON_PICKLE_PORT', metavar='<PORT>', type=int,
                        help='carbon pickle port')

    args = parser.parse_args()
    db_file = args.db_file
    CARBON_SERVER_IP = args.CARBON_SERVER_IP
    CARBON_PICKLE_PORT = args.CARBON_PICKLE_PORT

    # connect to graphite
    sock = socket.socket()
    try:
        sock.connect((CARBON_SERVER_IP, CARBON_PICKLE_PORT))
    except socket.error:
        raise SystemExit("Couldn't connect to %(server)s on port %(port)d, \
                              is graphite running in a docker environment?" % {'server': CARBON_SERVER_IP, 'port': CARBON_PICKLE_PORT})

    # send new stats to carbon server if available
    try:
        run(sock, db_file)
    except KeyboardInterrupt:
        sys.stderr.write("\nExiting on CTRL-c\n")
        sys.exit(0)


if __name__ == "__main__":
    main()
