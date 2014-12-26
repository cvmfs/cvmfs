import os
import cvmfs_geo

negative_expire_secs = 60*5;        # 5 minutes

def bad_request(start_response, reason):
    response_body = 'Bad Request: ' + reason + "\n"
    start_response('400 Bad Request',
                  [('Cache-control', 'max-age=' + str(negative_expire_secs)),
                   ('Content-Length', str(len(response_body)))])
    return [response_body]

def dispatch(api_func, path_info, repo_name, version, start_response, environ):
    if api_func == 'geo':
        return cvmfs_geo.api(path_info, repo_name, version, start_response, environ)

    return bad_request(start_response, 'unrecognized api function')
