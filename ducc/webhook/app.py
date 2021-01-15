from flask import Flask
from flask import request

import pprint

app = Flask(__name__)

@app.route("/<path:p>", methods=["POST"])
def catch_all(p):
    try:
        for (action, image) in handle_harbor(request.json):

            publish_message(action, image)

        return "ok"
    except Exception as e:
        print("Fail to handle the harbor hook")
        print(e)

    try:
        for (action, image) in handle_dockerhub(request.json):

            publish_message(action, image)

        return "ok"
    except Exception as e:
        print("Fail to handle the dockerhub hook")
        print(e)

    pprint.pprint(request.json)
    return "ok"

def publish_message(action, image):
    notification_file = f'{action}.notifications.txt'
    with open(notification_file, 'a+') as f:
        f.write(f'{image}\n')

    message = f'{action}|{image}'
    with open('notifications.txt', 'a+') as f:
        f.write(f'{message}\n')

    print(f'{action}|{image}')



def handle_dockerhub(rjson):
    for event in rjson['events']:
        action = event['action']
        # we need the protocol, the host, the repositor and the tag
        protocol = event['target']['url'].split(':')[0]
        host = event['request']['host']
        repository = event['target']['repository']
        tag = event['target'].get('tag', "")

        image = f'{protocol}://{host}/{repository}'
        if tag:
            image = f'{image}:{tag}'

        yield (action, image)

        notification_file = f'{action}.notifications.txt'
        with open(notification_file, 'a+') as f:
            f.write(f'{image}\n')

        message = f'{action}|{image}'
        with open('notifications.txt', 'a+') as f:
            f.write(f'{message}\n')

def handle_harbor(rjson):
    actions = {'PUSH_ARTIFACT': 'push'}
    for event in rjson['even_data']['resources']:
        resource_url = event['resource_url']
        image = f'https://{resource_url}'

        yield (actions[rjson['type']], image)


if __name__ == '__main__':
    app.run()
