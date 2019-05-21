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


def get_repos(args):
    rep = requests.get(args.gw_url + '/repos')
    print(json.dumps(rep.json()))


def toggle_repos(args):
    req = {'enable': bool(args.enable)}
    hmac_msg = json.dumps(req).encode()
    headers = {'authorization': args.key_id +
                ' ' + computeHMAC(hmac_msg, args.secret)}
    rep = requests.post(args.gw_url + '/repos/' +
                        args.repo_name, json=req, headers=headers)
    print(json.dumps(rep.json()))


def get_leases(args):
    rep = requests.get(args.gw_url + '/leases')
    print(json.dumps(rep.json()))


def get_lease(args):
    rep = requests.get(args.gw_url + '/leases/' + args.token)
    print(json.dumps(rep.json()))


def new_lease(args):
    req = {'path': args.path, 'api_version': '2'}
    hmac_msg = json.dumps(req).encode()
    headers = {'authorization': args.key_id +
                ' ' + computeHMAC(hmac_msg, args.secret)}
    rep = requests.post(args.gw_url + '/leases', json=req, headers=headers)
    print(json.dumps(rep.json()))


def cancel_lease(args):
    token = args.token
    hmac_msg = token.encode()
    headers = {'authorization': args.key_id +
                ' ' + computeHMAC(hmac_msg, args.secret)}
    rep = requests.delete(args.gw_url + '/leases/' + token, headers=headers)
    print(json.dumps(rep.json()))


def commit_lease(args):
    token = args.token
    hmac_msg = token.encode()
    headers = {'authorization': args.key_id +
                ' ' + computeHMAC(hmac_msg, args.secret)}
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
        hmac_msg = json.dumps(req).encode()
        req_url = args.gw_url + '/payloads'
    else:
        req = {'payload_digest': args.digest,
                'header_size': args.header_size,
                'api_version': '3'}
        hmac_msg = args.token.encode()
        req_url = args.gw_url + '/payloads' + args.token

    headers = {'authorization': args.key_id + ' ' + computeHMAC(hmac_msg, args.secret),
               'message-size': str(len(json.dumps(req)))}
    rep = requests.post(req_url, json=req, headers=headers)
    print(json.dumps(rep.json()))


def main():
    parser = argparse.ArgumentParser(description='Test gateway API requests')
    parser.add_argument('--gw_url', required=True,
                        help='URL of the repository gateway API root')
    parser.add_argument('--key_id', required=True,
                        help='Public ID of the secret key used to sign the request')
    parser.add_argument('--secret', required=True,
                        help='Secret key to be used to sign the request')
    subparsers = parser.add_subparsers(help='sub-command help')


    parser_get_repos = subparsers.add_parser(
        'get_repos', help='get a list of all repositories'
    )
    parser_get_repos.set_defaults(func=get_repos)


    parser_toggle_repo = subparsers.add_parser(
        'toggle_repo', help='enable or disable a repository')
    parser_toggle_repo.add_argument(
        '--enable', required=False, action='store_true', default=False, help='enabled state of the repository')
    parser_toggle_repo.add_argument(
        '--wait', required=False, action='store_true', default=False, help='wait until the repository can be disabled')
    parser_toggle_repo.add_argument(
        '--repo_name', required=True, help='name of the concerned repository')
    parser_toggle_repo.set_defaults(func=toggle_repos)


    parser_get_leases = subparsers.add_parser(
        'get_leases', help='get active leases'
    )
    parser_get_leases.set_defaults(func=get_leases)


    parser_get_lease = subparsers.add_parser(
        'get_lease', help='get an active lease'
    )
    parser_get_lease.add_argument('--token', required=True, help='lease token string')
    parser_get_lease.set_defaults(func=get_lease)


    parser_new_lease = subparsers.add_parser(
        'new_lease', help='request a new active lease'
    )
    parser_new_lease.add_argument('--path', required=True, help="lease path")
    parser_new_lease.set_defaults(func=new_lease)


    parser_cancel_lease = subparsers.add_parser(
        'cancel_lease', help='request a new active lease'
    )
    parser_cancel_lease.add_argument('--token', required=True, help='lease token string')
    parser_cancel_lease.set_defaults(func=cancel_lease)


    parser_commit_lease = subparsers.add_parser(
        'commit_lease', help='request a new active lease'
    )
    parser_commit_lease.add_argument('--token', required=True, help='lease token string')
    parser_commit_lease.add_argument('--old_hash', required=True, help='old root hash')
    parser_commit_lease.add_argument('--new_hash', required=True, help='new root hash')
    parser_commit_lease.set_defaults(func=commit_lease)


    parser_submit_payload = subparsers.add_parser(
        'submit_payload', help='request a new active lease'
    )
    parser_submit_payload.add_argument('--token', required=True, help='lease token string')
    parser_submit_payload.add_argument('--digest', required=True, help='payload digest')
    parser_submit_payload.add_argument('--header_size', required=True, help='payload object pack header size')
    parser_submit_payload.add_argument(
        '--legacy', required=False, default=False, action='store_true', help='use legacy request format')
    parser_submit_payload.set_defaults(func=submit_payload)

    parser.parse_args()


if __name__ == '__main__':
    main()