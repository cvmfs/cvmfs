#!/usr/bin/env python3

import argparse
import base64
import hmac
import json
import requests

def errMissingArg(argument):
    print('Missing argument: "{}"'.format(argument))

def computeHMAC(msg, key):
    d = hmac.HMAC(key, msg, digestmod='sha1').digest()
    return base64.b64encode(d).decode('utf-8')

parser = argparse.ArgumentParser(description='Test gateway API requests')
parser.add_argument(
    '--request', required=True,
    help='which command to perform (get_repos|get_leases|new_lease|cancel_lease|submit_payload|commit)')
parser.add_argument('--path', required=False, help="lease path")
parser.add_argument('--token', required=False, help='lease token string')
args = parser.parse_args()

base_url = 'http://localhost:4929/api/v1'

key_id = 'key1'
secret = b'secret1'

rep = None
if args.request == 'get_repos':
    rep = requests.get(base_url + '/repos')
elif args.request == 'get_leases':
    rep = requests.get(base_url + '/leases')
elif args.request == 'new_lease':
    if args.path:
        req = {'path':args.path,'api_version':2}
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

print(json.dumps(rep.json()))
