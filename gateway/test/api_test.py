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
        resp_js = json.loads(con.getresponse().read())
        print resp_js
    except Exception:
        pass

def main():
    base_res = '/api/'
    url_resp = [(base_res + '', 'GET', {}),
                (base_res + 'users', 'GET', {}),
                (base_res + 'repos', 'GET', {}),
                (base_res + 'leases', 'GET', {}),
                (base_res + 'leases', 'PUT', {'user' : 'user1', 'path' : '/path/to/repo/1'}),
                (base_res + 'leases', 'PUT', {'user' : 'bad_user', 'path' : '/path/to/repo/1'}),
                (base_res + 'leases', 'PUT', {'user' : 'user1', 'path' : '/bad/path'}),
                (base_res + 'leases', 'PUT', {'user' : 'user1', 'path' : '/path/to/repo/1'})]

    for (u, m, b) in url_resp:
        do_request(u, m, b)

if __name__ == '__main__':
    main()
