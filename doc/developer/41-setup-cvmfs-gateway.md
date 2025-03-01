# Setup of CernVM-FS Gateway

The idea behind the gateway is that only the gateway has access to CernVM-FS storage and that it coordinates all concurrent transactions via leases.
This allows multiple publishers to modify data in the same repository but at different data locations within the repository.

> **_WARNING_** &nbsp;
> - At the moment, the gateway does not support container publishing via *remote publishers*.
> - Transaction executed directly on the gateway are NOT protected by leases. **DO NOT DO THIS** unless you manually took a lease to protect the integrity of the gateway and prevent access from the publisher nodes (e.g. useful for garbage collection).

## Setup Gateway

Setting up a CernVM-FS gateway consists of 3 steps:

1. Create repository 
2. Create Gateway key that publisher will use for authentication
3. Make repository accessible via Gateway


### Example: Create gateway for repository `test.gw.repo`

(Note: every command is done in `sudo`)

1. **Create repository**

```bash
  # General setup (if you also want to run the client)
  cvmfs_config setup
  cvmfs_config chksetup

  # No autofs
  systemctl stop autofs

  # Setup Repo
  cvmfs_server mkfs test.gw.repo
```

2. **Create Gateway key**

- The key is written in plaintext
- Naming convention: `/etc/cvmfs/keys/<repo>.gw`

    - For this example: Content of `/etc/cvmfs/keys/test.gw.repo.gw` is
      ```
        plain_text mygwkey mysecret
      ```

- Modify access rights `chmod 600 test.gw.repo.gw`

3. **Make repository accessible via Gateway**

- Modify `/etc/cvmfs/gateway/repo.json` that it looks the following 

  ```json
    {
        "version": 2,
        "repos" : [
            "test.gw.repo"
        ]
    }
  ```

- Open Port 80 and 4929 (default gateway port) in the firewall, e.g. for `firewall-cmd`

  ```bash
    firewall-cmd --add-port=4929/tcp
    firewall-cmd --add-port=80/tcp

    # this should be then the result
    lsof -i -P -n | grep LISTEN
    cvmfs_gat 113011    root    7u  IPv6 249453      0t0  TCP *:4929 (LISTEN)
    cvmfs_gat 113011    root    8u  IPv4 251755      0t0  TCP 127.0.0.1:6060 (LISTEN)
  ```


## Setup Publisher

The publisher is set up like a normal `cvmfs_server` but with 2 changes

- It needs the gateway key
- `cvmfs_server mkfs` needs extra parameters to bind repository to gateway repository


### Example: Setting up publisher to access previously created gateway repository `test.gw.repo`

(Note: every command is done in `sudo`)

1. **General setup**

  ```bash
    # General setup (if you also want to run the client)
    cvmfs_config setup
    cvmfs_config chksetup

    # No autofs
    systemctl stop autofs
  ```

2. **Copy keys from gateway**

- Copy from `/etc/cvmfs/keys/` the following keys to a temporary folder `/home/myuser/tmpKeys/` on the publisher

  ```py
    test.gw.repo.crt
    test.gw.repo.gw
    test.gw.repo.pub
  ```


3. **Create repository linked to gateway repository**

- Create repository on publisher that is linked to the gateway repository

  ```bash
    # w = URL to storage location
    # u = URL to transaction and gateway server
    # k = temporary directory with keys
    # o = repo owner

    cvmfs_server mkfs \
    -w http://<URL to gateway>/cvmfs/test.gw.repo \
    -u gw,/srv/cvmfs/test.gw.repo/data/txn,http://<URL to gateway>:4929/api/v1 \
    -k /home/myuser/tmpKeys \
    -o root \
    test.gw.rep

    # for repo "s3.test.repo" with S3 storage backend
    # you need to change -w to the bucket url (might use https://)
    cvmfs_server mkfs \
        -w http(s)://<URL to S3 bucket> \
        -u gw,/srv/cvmfs/s3.test.repo/data/txn,http://<URL to gateway>:4929/api/v1 \
        -k /home/myuser/tmpKeys  \
        -o `whoami` \
        s3.test.repo
  ```

4. **Test access**

- Try first accessing using `curl`

  ```json
    curl http://<URL to gateway>:4929/api/v1/repos | jq

    # Output
    % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                    Dload  Upload   Total   Spent    Left  Speed
    100    79  100    79    0     0  39500      0 --:--:-- --:--:-- --:--:-- 39500
    {
    "data": {
        "test.gw.repo": {
        "keys": {
            "mygwkey": "/"
        },
        "enabled": true
        }
    },
    "status": "ok"
    }
  ```


- Execute a `cvmfs transaction` on the publisher

  ```bash
    cvmfs_server transaction test.gw.repo
    echo "hello" >> /cvmfs/test.gw.repo/msg.txt
    cvmfs_server publish test.gw.repo
  ```
