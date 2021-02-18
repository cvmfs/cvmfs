#!/usr/bin/env python3

import argparse
import base64
import hmac
import json
import requests
import time

# Utility functions

def compute_hmac(msg, key):
    d = hmac.HMAC(key, msg, digestmod='sha1').hexdigest().encode('utf-8')
    return base64.b64encode(d).decode('utf-8')

def make_headers(key_id, secret, msg):
    return {'authorization': key_id + ' ' + compute_hmac(msg.encode(), secret.encode())}

# Request functions follow

def get_repos(args):
    rep = requests.get(args.gw_url + '/repos')
    print(json.dumps(rep.json()))

def toggle_repo(args):
    req = {'enable': bool(args.enable)}
    headers = make_headers(args.key_id, args.secret, json.dumps(req))
    rep = requests.post(args.gw_url + '/repos/' +
                        args.repo, json=req, headers=headers)
    print(json.dumps(rep.json()))

def get_leases(args):
    rep = requests.get(args.gw_url + '/leases')
    print(json.dumps(rep.json()))

def get_lease(args):
    rep = requests.get(args.gw_url + '/leases/' + args.token)
    print(json.dumps(rep.json()))

def new_lease(args):
    req = {'path': args.path, 'api_version': '2'}
    headers = make_headers(args.key_id, args.secret, json.dumps(req))
    rep = requests.post(args.gw_url + '/leases', json=req, headers=headers)
    print(json.dumps(rep.json()))

def cancel_lease(args):
    token = args.token
    headers = make_headers(args.key_id, args.secret, token)
    rep = requests.delete(args.gw_url + '/leases/' + token, headers=headers)
    print(json.dumps(rep.json()))

def cancel_leases(args):
    prefix = args.prefix
    headers = make_headers(args.key_id, args.secret, '/api/v1/leases-by-path/' + prefix)
    rep = requests.delete(args.gw_url + '/leases-by-path/' + prefix, headers=headers)
    print(json.dumps(rep.json()))

def commit_lease(args):
    token = args.token
    headers = make_headers(args.key_id, args.secret, token)
    req = {'old_root_hash': args.old_hash,
           'new_root_hash': args.new_hash,
           'tag_name': 'mytag',
           'tag_channel': 'mychan',
           'tag_description': 'mydescription'}
    rep = requests.post(args.gw_url + '/leases/' + token,
                        json=req, headers=headers)
    print(json.dumps(rep.json()))

def submit_payload(args):
    hmac_msg = None
    req_url = None
    if args.legacy:
        req = {'session_token': args.token,
                'payload_digest': args.digest,
                'header_size': args.header_size,
                'api_version': '2'}
        hmac_msg = json.dumps(req)
        req_url = args.gw_url + '/payloads'
    else:
        req = {'payload_digest': args.digest,
                'header_size': args.header_size,
                'api_version': '3'}
        hmac_msg = args.token
        req_url = args.gw_url + '/payloads' + args.token

    headers = make_headers(args.key_id, args.secret, hmac_msg)
    rep = requests.post(req_url, json=req, headers=headers)
    print(json.dumps(rep.json()))

def gc(args):
    req = {
        'repo': args.repo,
        'num_revisions': args.num_revisions,
        'timestamp': args.timestamp,
        'dry_run': args.dry_run,
        'verbose': args.verbose
    }
    headers = make_headers(args.key_id, args.secret, json.dumps(req))
    rep = requests.post(args.gw_url + '/gc', json=req, headers=headers)
    print(json.dumps(rep.json()))

def publish_manifest(args):
    manifest = requests.get(args.manifest).text
    req = {
        'version': 1,
        'timestamp': time.strftime('%d %b %Y %H:%M:%S', time.gmtime()),
        'type': 'activity',
        'repository': args.repo,
        'manifest': base64.b64encode(manifest.encode('utf-8')).decode('utf-8')
    }
    rep = requests.post(args.gw_url + '/notifications/publish', json=req)
    print(json.dumps(rep.json()))

def subscribe(args):
    req = {
        'version': 1,
        'repository': args.repo
    }
    rep = requests.get(args.gw_url + '/notifications/subscribe', json=req, stream=True)

    for line in rep.iter_lines():
        decoded_line = line.decode('utf-8')
        msg = decoded_line[6:]
        print(json.dumps(msg))

def main():
    parser = argparse.ArgumentParser(description='Test gateway API requests')
    parser.add_argument('--gw_url', required=True,
                        help='URL of the repository gateway API root')
    parser.add_argument('--key_id', required=True,
                        help='Public ID of the secret key used to sign the request')
    parser.add_argument('--secret', required=True,
                        help='Secret key to be used to sign the request')
    subparsers = parser.add_subparsers(help='subcommands')
    subparsers.required = True
    subparsers.dest = 'command'

    subparsers.add_parser(
        'get_repos', help='get a list of all repositories'
    )

    parser_toggle_repo = subparsers.add_parser(
        'toggle_repo', help='enable or disable a repository')
    parser_toggle_repo.add_argument(
        '--enable', required=False, action='store_true', default=False, help='enabled state of the repository')
    parser_toggle_repo.add_argument(
        '--wait', required=False, action='store_true', default=False, help='wait until the repository can be disabled')
    parser_toggle_repo.add_argument(
        '--repo', required=True, help='name of the concerned repository')

    subparsers.add_parser(
        'get_leases', help='get active leases'
    )

    parser_get_lease = subparsers.add_parser(
        'get_lease', help='get an active lease'
    )
    parser_get_lease.add_argument('--token', required=True, help='lease token string')

    parser_new_lease = subparsers.add_parser(
        'new_lease', help='request a new active lease'
    )
    parser_new_lease.add_argument('--path', required=True, help="lease path")

    parser_cancel_lease = subparsers.add_parser(
        'cancel_lease', help='cancel a lease using a token'
    )
    parser_cancel_lease.add_argument('--token', required=True, help='lease token string')

    parser_cancel_leases = subparsers.add_parser(
        'cancel_leases', help='cancel all leases under a prefix'
    )
    parser_cancel_leases.add_argument('--prefix', required=True, help='repository+path prefix')

    parser_commit_lease = subparsers.add_parser(
        'commit_lease', help='commit a lease'
    )
    parser_commit_lease.add_argument('--token', required=True, help='lease token string')
    parser_commit_lease.add_argument('--old_hash', required=True, help='old root hash')
    parser_commit_lease.add_argument('--new_hash', required=True, help='new root hash')

    parser_submit_payload = subparsers.add_parser(
        'submit_payload', help='submit a payload'
    )
    parser_submit_payload.add_argument('--token', required=True, help='lease token string')
    parser_submit_payload.add_argument('--digest', required=True, help='payload digest')
    parser_submit_payload.add_argument('--header_size', required=True, help='payload object pack header size')
    parser_submit_payload.add_argument(
        '--legacy', required=False, default=False, action='store_true', help='use legacy request format')

    parser_gc = subparsers.add_parser(
        'gc', help='trigger garbage collection for a repository'
    )
    parser_gc.add_argument('--repo', required=True, help='name of the repository')
    parser_gc.add_argument('--num_revisions', required=False, help='number of revisions to keep')
    parser_gc.add_argument('--timestamp', required=False, help='threshold timestamp for garbage collection')
    parser_gc.add_argument('--dry_run', required=False, default=False, action='store_true', help='dry run')
    parser_gc.add_argument('--verbose', required=False, default=False, action='store_true', help='verbose output')

    parser_publish_manifest = subparsers.add_parser(
        'publish_manifest', help='publish a manifest to the notification system'
    )
    parser_publish_manifest.add_argument('--repo', required=True, help='name of the repository')
    parser_publish_manifest.add_argument('--manifest', required=True, help='URL of the repository manifest')

    parser_subscribe = subparsers.add_parser(
        'subscribe', help='subscribe to repository notifications'
    )
    parser_subscribe.add_argument('--repo', required=True, help="name of the repository")

    args = parser.parse_args()

    {
        'get_repos': get_repos,
        'toggle_repo': toggle_repo,
        'get_leases': get_leases,
        'get_lease': get_lease,
        'new_lease': new_lease,
        'cancel_lease': cancel_lease,
        'cancel_leases': cancel_leases,
        'commit_lease': commit_lease,
        'submit_payload': submit_payload,
        'gc': gc,
        'publish_manifest': publish_manifest,
        'subscribe': subscribe
    }[args.command](args)


if __name__ == '__main__':
    main()