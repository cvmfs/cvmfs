
from flask import Flask, request, send_from_directory
app = Flask(__name__)



@app.route('/<path:path>')
def root_get(path):
    # Check the Authorization header
    if 'Authorization' not in request.headers:
        return "No auth header present", 401
    if len(request.headers['Authorization'].split(" ", 1)) != 2:
        return "No auth header present", 401
    raw_token = request.headers['Authorization'].split(" ", 1)[1]
    if raw_token == "abcd1234":
        return send_from_directory('/srv/cvmfs/test.cern.ch/data', path)
    else:
        return "No auth header present", 401


