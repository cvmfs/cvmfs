#!/usr/bin/env python

import httplib
import json
import sys
import hmac
import base64
import hashlib

base_url = 'localhost'
port = 4929

api_root = '/api/v1'


def compute_hmac(secret, msg):
    return base64.b64encode(hmac.new(secret, msg, hashlib.sha1).hexdigest())


def do_request(url, method, body, headers):
    try:
        print url, method, body, ' => ',
        con = httplib.HTTPConnection(base_url, port)
        con.request(method, url, body, headers)
        return json.loads(con.getresponse().read())
    except Exception:
        pass


def create_and_delete_session():
    print 'Create and delete a session'
    print
    try:
        body = json.dumps({'path' : 'repo1.domain1.org', 'api_version' : '1'})
        body_hmac = compute_hmac('secret1', body)
        headers = {'Content-type' : 'application/json', 'authorization' : 'key1 ' + body_hmac}
        print 'Request - body: ' + body + ', hmac:' + body_hmac
        rep1 = do_request(api_root + '/leases', 'POST', body, headers)
        token = rep1['session_token']
        print 'New session: ', rep1
        print

        token_hmac = compute_hmac('secret1', token)
        headers = {'Content-type' : 'application/json', 'authorization' : 'key1 ' + token_hmac}
        rep2 = do_request(api_root + '/leases/' + token, 'DELETE', "", headers)
        print 'End session: ', rep2
        print
    except Exception, e:
        pass


def submit_payload():
    print 'Submit a payload in a new session'
    print
    try:
        body = json.dumps({'path' : 'repo1.domain1.org', 'api_version' : '1'})
        body_hmac = compute_hmac('secret1', body)
        headers = {'Content-type' : 'application/json', 'authorization' : 'key1 ' + body_hmac}
        print 'Request - body: ' + body + ', hmac:' + body_hmac
        rep1 = do_request(api_root + '/leases', 'POST', body, headers)
        token = rep1['session_token']
        print 'New session: ', rep1
        print

        payload = 'THIS_IS_A_PAYLOAD'
        digest = base64.b64encode('THIS_IS_THE_DIGEST_OF_THE_PAYLOAD')
        body = json.dumps({'session_token' : token,
                           'payload_digest' : digest,
                           'header_size' : '1',
                           'api_version' : '1'})
        body_hmac = compute_hmac('secret1', body)
        headers = {'Content-type' : 'application/json',
                   'authorization' : 'key1 ' + body_hmac,
                   'message-size' : str(len(body))}
        rep2 = do_request(api_root + '/payloads', 'POST', body, headers)
        print 'Payload submitted: ', rep2
        print

        token_hmac = compute_hmac('secret1', token)
        headers = {'Content-type' : 'application/json', 'authorization' : 'key1 ' + token_hmac}
        rep2 = do_request(api_root + '/leases/' + token, 'DELETE', "", headers)
        print 'End session: ', rep2
        print
    except Exception:
        pass

def main():
    create_and_delete_session()
    submit_payload()

if __name__ == '__main__':
    main()
