#!/usr/bin/env python3

import argparse
import base64
import hmac
import json
import requests

def computeHMAC(msg, key):
    d = hmac.HMAC(key, msg, digestmod='sha1').digest()
    return base64.b64encode(d).decode('utf-8')

parser = argparse.ArgumentParser(description='Test gateway API requests')
parser.add_argument('--request', required=True, help='which command to perform (get_repos|new_lease|drop_lease|submit_payload|commit)')
args = parser.parse_args()

print('Request: {}'.format(args.request))

base_url = 'http://localhost:4929/api/v1'

key_id = 'key1'
secret = b'secret1'

if args.request == 'repos':
    hmac_msg = b'/api/v1/repos'
    headers = {'authorization': key_id + ' ' + computeHMAC(hmac_msg, secret)}
    print('Headers: {}'.format(headers))
    rep = requests.get(base_url + '/repos', headers=headers)
    print('Reply: {}'.format(rep.json()))
elif args.request == 'new_lease':
    req = {'path':'/','api_version':2}
    hmac_msg = json.dumps(req).encode()
    headers = {'authorization': key_id + ' ' + computeHMAC(hmac_msg, secret)}
    rep = requests.post(base_url + '/leases', json=req, headers=headers)
    print('Reply: {}'.format(rep.json()))
