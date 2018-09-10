#!/usr/bin/python

# usage:
# python example-pickle-client.py [<NR_SECONDS_FOR_DELAY>]
# the stats.db SQLite local file should be in the same directory

# this is script is made to run continuously, at NR_SECONDS_FOR_DELAY interval (default 10s)


import re
import sys
import time
import socket
import pickle
import struct
import sqlite3
from calendar import timegm

CARBON_SERVER = '127.0.0.1'
CARBON_PICKLE_PORT = 2004
DELAY = 10

def get_data_publish_stats():
    # check in server.conf if CVMFS_STATISTICS_DB variable is set
    conn = sqlite3.connect('stats.db') # change path
    c = conn.cursor()

    c.execute('SELECT finished_time, files_added, files_removed, files_changed FROM publish_statistics ORDER BY publish_id DESC')
    return c.fetchmany(100)


def get_data_gc_stats():
    # check in server.conf if CVMFS_STATISTICS_DB variable is set
    conn = sqlite3.connect('stats.db') # change path
    c = conn.cursor()

    # todo: change this to take useful data from gc_statistics
    c.execute('SELECT finished_time, files_added, files_removed, files_changed FROM publish_statistics ORDER BY publish_id DESC')
    return c.fetchmany(10)


def run(sock, delay):
    while True:
        tuples = ([])
        lines = []

        data = get_data_publish_stats()

        # do this for the last 10 entries? # I will change this
        for x in xrange(1,100):
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
        # wait delay seconds and than take another measure
        time.sleep(delay)

def main():
    delay = DELAY
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        if arg.isdigit():
            delay = int(arg)
        else:
            sys.stderr.write("Ignoring non-integer argument. Using default: %ss\n" % delay)
    # connect to graphite
    sock = socket.socket()
    try:
        sock.connect( (CARBON_SERVER, CARBON_PICKLE_PORT) )
    except socket.error:
        raise SystemExit("Couldn't connect to %(server)s on port %(port)d, is graphite running in a docker environment?" % { 'server':CARBON_SERVER, 'port':CARBON_PICKLE_PORT })

    # send data continuously
    try:
        run(sock, delay)
    except KeyboardInterrupt:
        sys.stderr.write("\nExiting on CTRL-c\n")
        sys.exit(0)

if __name__ == "__main__":
    main()
