#!/usr/bin/python

# usage:
# python example-pickle-client.py [<NR_SECONDS_FOR_DELAY>]
# the stats.db SQLite local file should be in the same directory

# this is script is made to run continuously, at NR_SECONDS_FOR_DELAY interval (default 10s)

import argparse
import pickle
import re
import socket
import struct
import sqlite3
import sys
import time
from calendar import timegm

def get_data_publish_stats():
    # check in server.conf if CVMFS_STATISTICS_DB variable is set
    conn = sqlite3.connect('stats.db') # change path
    c = conn.cursor()

    c.execute('SELECT finished_time, files_added, files_removed, files_changed FROM publish_statistics ORDER BY publish_id DESC')
    return c.fetchmany(10)


def get_data_gc_stats():
    # check in server.conf if CVMFS_STATISTICS_DB variable is set
    conn = sqlite3.connect('stats.db') # change path
    c = conn.cursor()

    # todo: change this to take useful data from gc_statistics
    c.execute('SELECT finished_time, files_added, files_removed, files_changed FROM publish_statistics ORDER BY publish_id DESC')
    return c.fetchmany(10)


def run(sock):
    tuples = ([])
    lines = []

    data = get_data_publish_stats()

    # do this for the last 10 entries? # I will change this
    for x in xrange(1,10):
        time_obj = time.strptime(data[x][0], "%Y-%m-%d %H:%M:%S")
        timestamp_epoch = timegm(time_obj)
        tuples.append(('cvmfs.publish.files_added', (timestamp_epoch, data[x][1])))
        tuples.append(('cvmfs.publish.files_removed', (timestamp_epoch, data[x][2])))
        tuples.append(('cvmfs.publish.files_changed', (timestamp_epoch, data[x][3])))
        #just for DBG
        lines.append("cvmfs.publish.files_added %s %s" % (data[x][1], data[x][0]))
        lines.append("cvmfs.publish.files_removed %s %s" % (data[x][2], data[x][0]))
        lines.append("cvmfs.publish.files_changed %s %s" % (data[x][3], data[x][0]))
    
    #just for DBG
    message = '\n'.join(lines) + '\n' #all lines must end in a newline
    print("sending message")
    print('-' * 80)
    print(message)

    # build the package
    package = pickle.dumps(tuples, 1)
    size = struct.pack('!L', len(package))
    sock.sendall(size)
    sock.sendall(package)

def main():
    parser = argparse.ArgumentParser(description='Send stats to carbon server using pickle.')
    parser.add_argument('CARBON_SERVER_IP', metavar='<IP>', type=str,
                        help='carbon server ip')
    parser.add_argument('CARBON_PICKLE_PORT', metavar='<PORT>', type=int,
                        help='carbon pickle port')

    args = parser.parse_args()
    CARBON_SERVER_IP = args.CARBON_SERVER_IP
    CARBON_PICKLE_PORT = args.CARBON_PICKLE_PORT

    # connect to graphite
    sock = socket.socket()
    try:
        sock.connect( (CARBON_SERVER_IP, CARBON_PICKLE_PORT) )
    except socket.error:
        raise SystemExit("Couldn't connect to %(server)s on port %(port)d, is graphite running in a docker environment?" % { 'server':CARBON_SERVER_IP, 'port':CARBON_PICKLE_PORT })

    # send data continuously
    try:
        run(sock)
    except KeyboardInterrupt:
        sys.stderr.write("\nExiting on CTRL-c\n")
        sys.exit(0)

if __name__ == "__main__":
    main()
