# Webhook notification service interacting with cvmfs_ducc
The registry_webhook.py script listens to the webhook notifications from docker and harbor registries and appends to a notification file the image that was pushed, deleted or replicated. 
The notifications appended are of the form:
```
{id}|{action}|{image}
```
The registry-listener.go script checks for updates in the notification file and if finds a pushed image executes the cvmfs_ducc program with the convert-single-image option, so that after a few minutes the image is available on the cvmfs repository in both .flat (for Singularity/Apptainer use) and .layers (for Containerd/Kubernetes use) structures.   

Below an example of installation and run of both services on a Almalinux 9 server as almalinux user (with sudo privileges). It requires cvmfs_ducc and httpd already installed on the server.
```
sudo dnf install -y python3-mod_wsgi python3-flask python3-dotenv
cd /home/almalinux
git clone --branch devel --depth 1 https://github.com/cvmfs/cvmfs.git
cd cvmfs/ducc/webhook/; mkdir logs/
#
# set variables in .env file, if you change the PROJECT_PATH var and/or the user, change registry-listener.service accordingly
#
mod_wsgi-express-3 setup-server registry_webhook.wsgi --port 8080 --user almalinux --server-root=mod_wsgi-express-8080/ --log-directory logs/ --access-log
sudo cp registry-listener.service /etc/systemd/system/
mod_wsgi-express-8080/apachectl start
sudo systemctl start registry-listener
#
# check logs here:
tail -f logs/error_log
journalctl -f -u registry-listener -l
```
To enable https with self-signed certificate:
```
sudo dnf install -y mod_ssl
openssl genpkey -algorithm RSA -out hostkey.pem
openssl req -x509 -new -key hostkey.pem -out hostcert.pem -days 365
mod_wsgi-express-3 setup-server registry_webhook.wsgi  --https-port 8080 --https-only --server-name <server_name> --ssl-certificate-file hostcert.pem --ssl-certificate-key-file hostkey.pem --user almalinux --server-root=mod_wsgi-express-8080/ --log-directory logs/ --access-log
```
To run in production mode:
```
#
# set variables in .env file, enable https and change registry-listener.service and registry-webhook.service accordingly
#
sudo cp registry-listener.service /etc/systemd/system/
sudo cp registry-webhook.service /etc/systemd/system/
sudo systemctl enable registry-webhook && sudo systemctl start registry-webhook
sudo systemctl enable registry-listener && sudo systemctl start registry-listener
#
# check logs here:
tail -f logs/error_log
journalctl -f -u registry-webhook -l
journalctl -f -u registry-listener -l
```
