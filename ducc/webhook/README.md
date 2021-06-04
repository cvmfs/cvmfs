# Webhook notification service

This small script listens to the webhooks notifications from docker and harbor registries and appends to a file the image that was pushed, deleted or replicated.

Run with:

```
python3 registry-webhook.py -f "notifications.txt" -h 188.185.120.234 -p 8080
```
where the flags account for:
* `--file`(`-f'): name of the file where the notifications are appended.
* `--host`(`-h'): name of the server.
* `--port`(`-p'): port number to listen on.

The notifications appended are of the form:
```
{id}|{action}|{image}
``
