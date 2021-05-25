FLASK_APP=app.py python3 -m flask run -h 188.185.120.234 -p 8080 &

sleep 1

pid=$(echo $!)

check_id_action() {
    expected_id=$1
    expected_action=$2

    notification=$( tail -n 1 notifications.txt )
    id=(${notification//|/ })

    if [ ${id[0]} != ${expected_id} ]
    then
        echo 'The id of the action is not correct'
        kill $pid
        exit 1
    fi

    if [ ${id[1]} != ${expected_action} ]
    then
        echo 'The action not correct'
        kill $pid
        exit 1
    fi

    sleep 1
}

send_push() {
    curl -X POST http://webhook-c8vm:8080/webhooks/test-cvmfs-ingestion -H "Content-Type: application/json" -d "{\"type\": \"PUSH_ARTIFACT\", \"event_data\": {\"resources\": [{\"resource_url\": \"registry.cern.ch/unpacked-dev/image:tag\"}]}}"
}

send_delete() {
    curl -X POST http://webhook-c8vm:8080/webhooks/test-cvmfs-ingestion -H "Content-Type: application/json" -d "{\"type\": \"DELETE_ARTIFACT\", \"event_data\": {\"resources\": [{\"resource_url\": \"registry.cern.ch/unpacked-dev/image:tag\"}]}}"
}

send_replication() {
    curl -X POST http://webhook-c8vm:8080/webhooks/test-cvmfs-ingestion -H "Content-Type: application/json" -d "{\"type\": \"REPLICATION\", \"event_data\": {\"replication\": { \"dest_resource\": {\"endpoint\": \"https://registry.cern.ch\", \"namespace\": \"unpacked-dev\"}, \"successful_artifact\": [{\"type\": \"image\", \"status\": \"Success\", \"name_tag\": \"image:tag\"}]}}}"
}

yes | rm 'notifications.txt' || true
touch notifications.txt

send_push
check_id_action '0' 'push'

send_delete
check_id_action '1' 'delete'

send_replication
check_id_action '2' 'replication'

yes | rm 'notifications.txt' || true
touch notifications.txt

send_delete
check_id_action '0' 'delete'

send_replication
check_id_action '1' 'replication'

send_replication
check_id_action '2' 'replication'

send_push
check_id_action '3' 'push'

yes | rm 'notifications.txt' || true
touch notifications.txt

send_replication
check_id_action '0' 'replication'

send_push
check_id_action '1' 'push'

send_push
check_id_action '2' 'push'

send_push
check_id_action '3' 'push'

send_delete
check_id_action '4' 'delete'

send_delete
check_id_action '5' 'delete'

send_delete
check_id_action '6' 'delete'

echo "999|push|https://registry.cern.ch/unpacked-dev/image:tag" >> notifications.txt

send_delete
check_id_action '1000' 'delete'

send_push
check_id_action '1001' 'push'

if [ ! -f "notifications1000.txt" ]; then
    echo -e "\nAn error ocurred during file rotation."
    kill $pid
    exit 1
fi

echo "2000|push|https://registry.cern.ch/unpacked-dev/image:tag" >> notifications.txt

send_push
check_id_action '2001' 'push' 

if [ ! -f "notifications2000.txt" ]; then
    echo -e "\nAn error ocurred during file rotation."
    kill $pid
    exit 1
fi

kill $pid

echo -e "\nTest finished successfully"
