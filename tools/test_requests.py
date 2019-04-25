#!/usr/bin/env python3

import argparse
import base64
import hmac
import json
import requests

def errMissingArg(argument):
    print('Missing argument: "{}"'.format(argument))

def computeHMAC(msg, key):
    d = hmac.HMAC(key, msg, digestmod='sha1').hexdigest().encode('utf-8')
    return base64.b64encode(d).decode('utf-8')

parser = argparse.ArgumentParser(description='Test gateway API requests')
parser.add_argument(
    '--request', required=True,
    help='which command to perform (get_repos|get_leases|new_lease|cancel_lease|submit_payload|commit)')
parser.add_argument('--path', required=False, help="lease path")
parser.add_argument('--token', required=False, help='lease token string')
parser.add_argument('--old_hash', required=False, help='old root hash')
parser.add_argument('--new_hash', required=False, help='new root hash')
parser.add_argument('--digest', required=False, help='payload digest')
parser.add_argument('--header_size', required=False, help='payload (object pack) header size')
args = parser.parse_args()

base_url = 'http://localhost:4929/api/v1'

key_id = 'key1'
secret = b'secret1'

rep = None
if args.request == 'get_repos':
    rep = requests.get(base_url + '/repos')

elif args.request == 'get_leases':
    rep = requests.get(base_url + '/leases')

elif args.request == 'get_lease':
    if args.token:
        token = args.token
        rep = requests.get(base_url + '/leases/' + token)

elif args.request == 'new_lease':
    if args.path:
        req = {'path':args.path,'api_version':'2'}
        hmac_msg = json.dumps(req).encode()
        headers = {'authorization': key_id + ' ' + computeHMAC(hmac_msg, secret)}
        rep = requests.post(base_url + '/leases', json=req, headers=headers)
    else:
        errMissingArg('--path')

elif args.request == 'cancel_lease':
    if args.token:
        token = args.token
        hmac_msg = token.encode()
        headers = {'authorization': key_id + ' ' + computeHMAC(hmac_msg, secret)}
        rep = requests.delete(base_url + '/leases/' + token, headers=headers)
    else:
        errMissingArg('--token')

elif args.request == 'commit_lease':
    if args.token and args.old_hash and args.new_hash:
        token = args.token
        hmac_msg = token.encode()
        headers = {'authorization': key_id + ' ' + computeHMAC(hmac_msg, secret)}
        req = {'old_root_hash': args.old_hash,
               'new_root_hash': args.new_hash,
               'tag_name': 'mytag',
               'tag_channel': 'mychan',
               'tag_description': 'mydescription'}
        rep = requests.post(base_url + '/leases/' + token, json=req, headers=headers)
    else:
        errMissingArg('--token')
        errMissingArg('--old_hash')
        errMissingArg('--new_hash')

elif args.request == 'payload_legacy':
    if args.token and args.digest and args.header_size:
        req = {'session_token': args.token,
               'payload_digest': args.digest,
               'header_size': args.header_size,
               'api_version': '2'}
        hmac_msg = json.dumps(req).encode()
        headers = {'authorization': key_id + ' ' + computeHMAC(hmac_msg, secret),
                   'message-size': str(len(json.dumps(req)))}
        rep = requests.post(base_url + '/payloads', json=req, headers=headers)
    else:
        errMissingArg('--token')
        errMissingArg('--digest')
        errMissingArg('--header_size')

elif args.request == 'payload':
    if args.token and args.digest and args.header_size:
        hmac_msg = args.token.encode()
        req = {'payload_digest': args.digest,
               'header_size': args.header_size,
               'api_version': '3'}
        headers = {'authorization': key_id + ' ' + computeHMAC(hmac_msg, secret),
                   'message-size': str(len(json.dumps(req)))}
        rep = requests.post(base_url + '/payloads/' + args.token, json=req, headers=headers)
    else:
        errMissingArg('--token')
        errMissingArg('--digest')
        errMissingArg('--header_size')

print(json.dumps(rep.json()))
