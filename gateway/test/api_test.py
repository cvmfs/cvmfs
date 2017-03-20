#!/usr/bin/env python

import httplib
import json
import sys

base_url = 'localhost'
port = 8080

api_root = '/api/v1'

def do_request(url, method, body):
    try:
        print url, method, ' => ',
        con = httplib.HTTPConnection(base_url, port)
        headers = {'Content-type' : 'application/json'}
        con.request(method, url, json.dumps(body), headers)
        return json.loads(con.getresponse().read())
    except Exception:
        pass

def create_and_delete_session():
    print 'Create and delete a session'
    try:
        rep1 = do_request(api_root + '/leases', 'POST', {'key_id' : 'key1', 'path' : 'repo1.domain1.org'})
        print 'New session: ', rep1
        token = rep1['session_token']
        rep2 = do_request(api_root + '/leases/' + token, 'DELETE', {})
        print 'End session: ', rep2
        print
    except Exception:
        pass

def submit_payload():
    print 'Submit a payload in a new session'
    try:
        rep1 = do_request(api_root + '/leases', 'POST', {'key_id' : 'key1', 'path' : 'repo1.domain1.org'})
        print 'New session: ', rep1
        token = rep1['session_token']
        rep2 = do_request(api_root + '/payloads', 'POST', {'session_token' : token, 'payload' : 'abcd'})
        print 'Payload submitted: ', rep2
        rep2 = do_request(api_root + '/leases/' + token, 'DELETE', {})
        print 'End session: ', rep2
        print
    except Exception:
        pass

def main():
    create_and_delete_session()

    submit_payload()

    print 'Basic tests'
    url_resp = [(api_root + '', 'GET', {}),
                (api_root + '/repos', 'GET', {}),
                (api_root + '/leases', 'GET', {}),
                (api_root + '/leases', 'POST', {'key_id' : 'key1', 'path' : 'repo1.domain1.org'}),
                (api_root + '/leases', 'POST', {'key_id' : 'bad_user', 'path' : 'repo1.domain1.org'}),
                (api_root + '/leases', 'POST', {'key_id' : 'key1', 'path' : '/bad/path'}),
                (api_root + '/leases', 'POST', {'key_id' : 'key1', 'path' : 'repo1.domain1.org'})]
    rep = []
    for (u, m, b) in url_resp:
        r = do_request(u, m, b)
        rep.append(r)
        print r
    token = rep[3]['session_token']
    do_request(api_root + '/leases/' + token, 'DELETE', {})
    print

if __name__ == '__main__':
    main()
