from flask import Flask
from flask import request

import argparse
import pprint
import os

app = Flask(__name__)

@app.route("/<path:p>", methods=["POST"])
def catch_all(p):
    print(request.json)
    try:
        for (action, image) in handle_harbor(request.json):
            publish_message(notifications_file, action, image)
        return "ok"
    except Exception as e:
        print("Fail to handle the harbor hook")
        print(e)

    try:
        for (action, image) in handle_dockerhub(request.json, notifications_file):

            publish_message(notifications_file, action, image)

        return "ok"
    except Exception as e:
        print("Fail to handle the dockerhub hook")
        print(e)

    pprint.pprint(request.json)
    return "ko", 500

def publish_message(notifications_file, action, image):
    with open(notifications_file, 'a+') as f:
        if os.stat(notifications_file).st_size == 0:
            current_id = 0
        else:
            f.seek(0)
            lines = f.readlines()
            first_line = lines[0]
            first_line_id = int(first_line.split('|')[0])
            last_line = lines[-1]
            last_line_id = int(last_line.split('|')[0])
            current_id = last_line_id + 1
            if (last_line_id % int(args_dic["rotation"]) == 0 and last_line_id != 0):
                new_notifications_file = str(first_line_id)+"-"+str(last_line_id)+notifications_file
                os.rename(notifications_file, new_notifications_file)
                with open(new_notifications_file, 'a+') as f:
                    f.write(f'xx|file rotation|xx\n')
                with open(notifications_file, 'a+') as f:
                    message = f'{str(current_id)}|{action}|{image}'
                    f.write(f'{message}\n')
                    return

        message = f'{str(current_id)}|{action}|{image}'
        f.write(f'{message}\n')

    print(f'{action}|{image}')


def handle_dockerhub(rjson, notifications_file):
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

        message = f'{action}|{image}'

        yield (action, image)


def handle_harbor(rjson):
    actions = {'PUSH_ARTIFACT': 'push', 'DELETE_ARTIFACT': 'delete', 'REPLICATION': 'replication'}
    action = actions[rjson['type']]
    if action == 'push' or action == 'delete':
        for event in rjson['event_data']['resources']:
            resource_url = event['resource_url']
            image = f'https://{resource_url}'
            yield (action, image)

    elif action == 'replication':
        replication = rjson['event_data']['replication']
        registry_dst = replication["dest_resource"]
        registry_src = replication["src_resource"]
        destination = f'{registry_dst["endpoint"]}/{registry_dst["namespace"]}'
        source = f'{registry_src["endpoint"]}/{registry_src["namespace"]}'
        try:
            artifact = rjson['event_data']['replication']['successful_artifact']
            for event in artifact:
                image_name = event['name_tag'].split(" ")[0]
            image = str(destination + '/' + image_name)
            yield (action, image)
        except Exception as e:
            print("The replicated artifact from {} already exists in {}".format(str(source), str(destination)))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("-f", "--file")
    parser.add_argument("-h", "--host")
    parser.add_argument("-p", "--port")
    parser.add_argument("-r", "--rotation")
    args = parser.parse_args()
    args_dic = vars(args)
    notifications_file = args_dic["file"]

    app.run(host=args_dic["host"], port=args_dic["port"])
