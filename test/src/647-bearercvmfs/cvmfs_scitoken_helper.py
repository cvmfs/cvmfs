#!/usr/bin/env python3


# This is a simple echo script

import sys
import json
import struct
import re
import os

PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3

def ReadMsg():

    try:
        # python2 compatibility
        if PY3:
            msg = sys.stdin.buffer.read(8)
        else:
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
    return json_obj
    

def GetToken(message):
    f = open("/tmp/scitoken.out", 'a')
    f.write(str(message))
    # Get the _CONDOR_CREDS directory from the process's environment
    env_file = "/proc/{0}/environ".format(message['pid'])
    f.write("Environ file: %s\n" % env_file)
    
    # Look for the _CONDOR_CREDS directory setting in the env file
    env_file_obj = open(env_file, 'r')
    f.write(env_file_obj.read())
    env_file_obj.seek(0)
    re_match = re.search("\x00_CONDOR_CREDS=([\w\s\/]+)\x00", env_file_obj.read())
    if not re_match:
        f.write("Didn't find _CONDOR_CREDS\n")
        f.close()
        return None
    creds_dir = re_match.group(1)
    f.write("creds dir = %s\n" % creds_dir)
    token_path = os.path.join(creds_dir, "scitoken.use")
    if not os.path.exists(token_path):
        f.write("Token path: %s does not exist\n" % token_path)
        return None
    else:
        # Read in the token
        token = open(token_path, 'r').read()
        f.write("Found token: %s\n" % token)
        return token.strip()
    f.close()


def WriteMsg(msg):
    json_str = json.dumps(msg)
    packed = struct.pack("II", 1, len(json_str))
    if PY3:
        sys.stdout.buffer.write(packed)
        sys.stdout.buffer.write(bytes(json_str,'utf-8'))
    else:
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
    recv_msg = ReadMsg()
    if recv_msg['cvmfs_authz_v1']['msgid'] == 4:
        sys.exit(0)
    token = GetToken(recv_msg['cvmfs_authz_v1'])


    # Always allow
    msg = {}
    if token:
    	msg['cvmfs_authz_v1'] = {'msgid': 3, 'revision': 0, 'status': 0, 'bearer_token': token}
    else:
        msg['cvmfs_authz_v1'] = {'msgid': 3, 'revision': 0, 'status': 2}
    WriteMsg(msg)
