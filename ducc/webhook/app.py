from flask import Flask
from flask import request

import pprint
import os

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
    return "ko", 500

def publish_message(action, image):

    with open('notifications.txt', 'a+') as f:
        if os.stat('notifications.txt').st_size == 0:
            current_id = 0
        else:
            f.seek(0)
            last_line = f.readlines()[-1]
            last_line_id = last_line.split('|')[0]
            current_id = int(last_line_id) + 1
            if (int(last_line_id) % 1000 == 0 and int(last_line_id) != 0):
                f.write(f'--- FILE ROTATION ---\n')
                os.rename('notifications.txt', 'notifications'+str(last_line_id)+'.txt')
                with open('notifications.txt', 'a+') as f:
                    message = f'{str(current_id)}|{action}|{image}'
                    f.write(f'{message}\n')
                    return
        message = f'{str(current_id)}|{action}|{image}'
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

        message = f'{action}|{image}'
        with open('notifications.txt', 'a+') as f:
            f.write(f'{message}\n')

def handle_harbor(rjson):
    print(rjson)
    actions = {'PUSH_ARTIFACT': 'push', 'DELETE_ARTIFACT': 'delete', 'REPLICATION': 'replication'}
    action = actions[rjson['type']]
    if action == 'push' or action == 'delete':
        for event in rjson['event_data']['resources']:
            resource_url = event['resource_url']
            image = f'https://{resource_url}'
            yield (action, image)

    elif action == 'replication':
        replication = rjson['event_data']['replication']
        registry_info = replication["dest_resource"]
        destination = f'{registry_info["endpoint"]}/{registry_info["namespace"]}'
        try:
            artifact = rjson['event_data']['replication']['successful_artifact']
            for event in artifact:
                image_name = event['name_tag'].split(" ")[0]
            image = str(destination + '/' + image_name)
            yield (action, image)
        except Exception as e:
            print("The replicated artifact already exists")

if __name__ == '__main__':
    app.run()
