#!/usr/bin/python3

import os
import sys
import requests

if len(sys.argv) == 1 or len(sys.argv) != 3:
    # Only the invocation
    # we print a simple help
    print("Utility to print to STDIN all tha images stored in an harbor repository")
    print("Pass as first argument the url with protocol of the harbor host")
    print("As second argument pass the name of the project")
    print("The username and password are to be provide as env var. HARBOR_{USER, PASS}")
    sys.exit(0)

user = os.getenv('HARBOR_USER')
if user is None:
    print("Need an username to scan the repository, set the env variable `HARBOR_USER`")
    sys.exit(1)

password = os.getenv('HARBOR_PASS')
if password is None:
    print("Need a password to scan the repository, set the env variable `HARBOR_PASS`")
    sys.exit(1)

url = sys.argv[1]
project_name = sys.argv[2]

if not url.startswith('http'):
    url = 'https://' + url

req_url = f"{url}/api/v2.0/projects/{project_name}/repositories"

resp = requests.get(req_url, auth=(user, password), params={'page': 1, 'page_size': 50})

if resp.status_code != 200:
    print("Error in making the requests")
    print("Response: ", resp.json())
    sys.exit(1)

repositories = resp.json()

while resp.links.get('next'):
    next_link = resp.links['next']['url']
    req_url = f"{url}/{next_link}"
    resp = requests.get(req_url, auth=(user, password))

    if resp.status_code != 200:
        break
    
    repositories += resp.json()

for repo in repositories:
    print(f"{url}/{repo['name']}")

