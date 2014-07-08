#! /usr/bin/env python

import os, sys, re
import cvmfs_api

pattern = re.compile('^/([^/]*)/(v[^/]*)/([^/]*)/(.*)$')

def application(environ, start_response):
    request_url  = environ['PATH_INFO']
    match_result = pattern.search(request_url)

    if not match_result:
        return cvmfs_api.bad_request(start_response, 'malformed api URL: ' + request_url)

    repo_name, version, api_func, path_info = match_result.groups()

    return cvmfs_api.dispatch(api_func, path_info, repo_name, version, start_response, environ)
