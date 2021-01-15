# Webhook notification service

This small script listen to webhooks from the docker registry and append to a file the docker image that was pushed

Run with

```
FLASK_APP=app.py flask run --port=8080
```
