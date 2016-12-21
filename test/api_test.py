#!/usr/bin/env python

import httplib
import json
import sys

base_url = 'localhost'
port = 8080

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
    try:
        rep1 = do_request('/api/leases', 'POST', {'user' : 'user1', 'path' : 'repo1.domain1.org'})
        print 'New session: ', rep1
        token = rep1['session_token']
        rep2 = do_request('/api/leases/' + token, 'DELETE', {})
        print 'End session: ', rep2
    except Exception:
        pass

def main():
    base_res = '/api/'
    url_resp = [(base_res + '', 'GET', {}),
                (base_res + 'users', 'GET', {}),
                (base_res + 'repos', 'GET', {}),
                (base_res + 'leases', 'GET', {}),
                (base_res + 'leases', 'POST', {'user' : 'user1', 'path' : 'repo1.domain1.org'}),
                (base_res + 'leases', 'POST', {'user' : 'bad_user', 'path' : 'repo1.domain1.org'}),
                (base_res + 'leases', 'POST', {'user' : 'user1', 'path' : '/bad/path'}),
                (base_res + 'leases', 'POST', {'user' : 'user1', 'path' : 'repo1.domain1.org'})]

    create_and_delete_session()

    for (u, m, b) in url_resp:
        rep = do_request(u, m, b)
        print rep


if __name__ == '__main__':
    main()
