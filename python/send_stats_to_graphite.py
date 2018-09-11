#!/usr/bin/python

import argparse
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

    # the following file must have two timestamps (one per line):
    # last_start_time_sent_for_publish_statistics
    # last_start_time_sent_for_gc_statistics

    # todo: change path to `/var/spool/cvmfs/<REPO>/last_timestamp_sent`
    f = open("aux.txt", "r+")

    # read first two lines
    publish_timestamp = f.readline()[:-1]  # delete the newline character
    gc_timestamp = f.readline()

    f.truncate()  # delete file content
    f.seek(0)     # move file cursor

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
            # #just for DBG
            # lines.append("cvmfs.publish.files_added %s %s" % (publish_stats[x][1], publish_stats[x][0]))
            # lines.append("cvmfs.publish.files_removed %s %s" % (publish_stats[x][2], publish_stats[x][0]))
            # lines.append("cvmfs.publish.files_changed %s %s" % (publish_stats[x][3], publish_stats[x][0]))
            # lines.append("cvmfs.publish.duplicated_files %s %s" % (publish_stats[x][4], publish_stats[x][0]))
            # lines.append("cvmfs.publish.directories_added %s %s" % (publish_stats[x][5], publish_stats[x][0]))
            # lines.append("cvmfs.publish.directories_removed %s %s" % (publish_stats[x][6], publish_stats[x][0]))
            # lines.append("cvmfs.publish.directories_changed %s %s" % (publish_stats[x][7], publish_stats[x][0]))
            # lines.append("cvmfs.publish.sz_bytes_added %s %s" % (publish_stats[x][8], publish_stats[x][0]))
            # lines.append("cvmfs.publish.sz_bytes_removed %s %s" % (publish_stats[x][9], publish_stats[x][0]))
            # lines.append("cvmfs.publish.sz_bytes_uploaded %s %s" % (publish_stats[x][10], publish_stats[x][0]))

    if len(gc_stats) > 0:
        gc_timestamp = gc_stats[0][0]
        for x in xrange(0, len(gc_stats)):
            time_obj = time.strptime(gc_stats[x][0], "%Y-%m-%d %H:%M:%S")
            timestamp_epoch = timegm(time_obj)
            tuples.append(('cvmfs.gc.n_preserved_catalogs', (timestamp_epoch, gc_stats[x][1])))
            tuples.append(('cvmfs.gc.n_condemned_catalogs', (timestamp_epoch, gc_stats[x][2])))
            tuples.append(('cvmfs.gc.n_condemned_objects', (timestamp_epoch, gc_stats[x][3])))
            tuples.append(('cvmfs.gc.sz_condemned_bytes', (timestamp_epoch, gc_stats[x][4])))
            # #just for DBG
            # lines.append("cvmfs.gc.n_preserved_catalogs %s %s" % (gc_stats[x][1], gc_stats[x][0]))
            # lines.append("cvmfs.gc.n_condemned_catalogs %s %s" % (gc_stats[x][2], gc_stats[x][0]))
            # lines.append("cvmfs.gc.n_condemned_objects %s %s" % (gc_stats[x][3], gc_stats[x][0]))
            # lines.append("cvmfs.gc.sz_condemned_bytes %s %s" % (gc_stats[x][3], gc_stats[x][0]))

    # #just for DBG
    # message = '\n'.join(lines) + '\n' #all lines must end in a newline
    # print("sending message")
    # print('-' * 80)
    # print(message)

    # build the package
    package = pickle.dumps(tuples, 1)
    size = struct.pack('!L', len(package))
    sock.sendall(size)
    sock.sendall(package)

    # write the last finished_time into a file
    f.write(publish_timestamp + "\n")  # write last publish start_time
    f.write(gc_timestamp)              # write last gc start_time
    f.close()


def main():
    parser = argparse.ArgumentParser(description='Send stats to carbon server using pickle.')
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
        raise SystemExit("Couldn't connect to %(server)s on port %(port)d, is graphite running in a docker environment?" % {'server': CARBON_SERVER_IP, 'port': CARBON_PICKLE_PORT})

    # send new stats to carbon server if available
    try:
        run(sock, db_file)
    except KeyboardInterrupt:
        sys.stderr.write("\nExiting on CTRL-c\n")
        sys.exit(0)


if __name__ == "__main__":
    main()
