from flask import Flask
from flask import request

import pprint

app = Flask(__name__)

@app.route("/<path>", methods=["POST"])
def catch_all(path):
    # pprint.pprint(request.json)
    for event in request.json['events']:
        action = event['action']
        # we need the protocol, the host, the repositor and the tag
        protocol = event['target']['url'].split(':')[0]
        host = event['request']['host']
        repository = event['target']['repository']
        tag = event['target'].get('tag', "")

        image = f'{protocol}://{host}/{repository}'
        if tag:
            image = f'{image}:{tag}'


        notification_file = f'{action}.notifications.txt'
        with open(notification_file, 'a+') as f:
            f.write(f'{image}\n')

        message = f'{action}|{image}'
        with open('notifications.txt', 'a+') as f:
            f.write(f'{message}\n')

        print(f'{action}|{image}')
    return "ok"

if __name__ == '__main__':
    app.run()
