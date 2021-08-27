# Webhook notification service

This small script listens to the webhooks notifications from docker and harbor registries and appends to a file the image that was pushed, deleted or replicated.

Run with:

```
python3 registry-webhook.py -f "notifications.txt" -h 127.0.0.1 -p 8080
```
where the flags account for:
* `--file`(`-f'): name of the file where the notifications are appended.
* `--host`(`-h'): IP address of the interface to bind to.
* `--port`(`-p'): port number to listen on.
* `--rotation`(`-r`): maximum action id per file.

The notifications appended are of the form:
```
{id}|{action}|{image}
``
