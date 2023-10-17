# Webhook notification service interacting with cvmfs_ducc
The registry_webhook.py script listens to the webhook notifications from docker and harbor registries and appends to a notification file the image that was pushed, deleted or replicated. 
The notifications appended are of the form:
```
{id}|{action}|{image}
```
The registry-listener.go script checks for updates in the notification file and if finds a pushed image executes the cvmfs_ducc program with the convert-single-image option, so that after a few minutes the image is available on the cvmfs repository in both .flat (for Singularity/Apptainer use) and .layers (for Containerd/Kubernetes use) structures.   

Below an example of installation and run of both services on a centos7 server as centos user (with sudo privileges). It requires cvmfs_ducc and httpd already installed on the server.
```
sudo yum install -y python36-mod_wsgi python3-flask
sudo pip3 install python-dotenv
cd /home/centos
git clone --branch devel --depth 1 https://github.com/cvmfs/cvmfs.git
cd cvmfs/ducc/webhook/
#
# set variables in .env file, if you change the PROJECT_PATH var and/or the user, change registry-listener.service accordingly
#
mod_wsgi-express-3 setup-server registry_webhook.wsgi --port 8080 --user centos --server-root=mod_wsgi-express-8080/ --log-directory logs/ --access-log
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
sudo yum install -y mod_ssl
openssl genpkey -algorithm RSA -out server.key
openssl req -x509 -new -key server.key -out server.crt -days 365
mod_wsgi-express-3 setup-server registry_webhook.wsgi  --https-port 8080 --https-only --server-name <server_name> --ssl-certificate-file server.crt --ssl-certificate-key-file server.key --user centos --server-root=mod_wsgi-express-8080/ --log-directory logs/ --access-log
```
