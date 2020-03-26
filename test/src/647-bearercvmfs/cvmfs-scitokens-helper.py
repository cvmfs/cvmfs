#!/usr/bin/env python2


# This is a simple echo script

import sys
import json
import struct


def ReadMsg():

    try:
        msg = sys.stdin.read(8)
        if len(msg) != 8:
            return
        (version, size) = struct.unpack("II", msg)
    except Exception as e:
        raise e
    
    try:
        json_raw = str(sys.stdin.read(size))
    except Exception as e:
        raise e
    
    try:
        json_obj = json.loads(json_raw)
    except Exception as e:
        raise e
    

def WriteMsg(msg):
    json_str = json.dumps(msg)
    packed = struct.pack("II", 1, len(json_str))
    sys.stdout.write(packed)
    sys.stdout.write(json_str)
    sys.stdout.flush()

# Read the initialization
ReadMsg()

# Send the handshake
msg = {}
msg['cvmfs_authz_v1'] = {'msgid': 1, 'revision': 0}
WriteMsg(msg)


while True:
    ReadMsg()

    # Always allow
    msg = {}
    msg['cvmfs_authz_v1'] = {'msgid': 3, 'revision': 0, 'status': 0, 'bearer_token': "abcd1234"}
    WriteMsg(msg)





