#!/usr/bin/env python

import httplib
import json

base_url = 'localhost'
port = 8080

def do_request(url, resp_key):
    print url, resp_key
    con = httplib.HTTPConnection(base_url, port)
    headers = {'Content-type' : 'application/json'}
    con.request('GET', url, json.dumps({}), headers)
    resp_js = json.loads(con.getresponse().read())
    print resp_js[resp_key]


def main():
    base_res = '/api/'
    url_resp = [(base_res + '', 'resources'),
                (base_res + 'users', 'users'),
                (base_res + 'repos', 'repos')]

    for (u, r) in url_resp:
        do_request(u, r)

if __name__ == '__main__':
    main()
