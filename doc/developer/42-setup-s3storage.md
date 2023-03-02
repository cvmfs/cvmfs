# Using S3 Storage with CernVM-FS Server

It is possible to use S3 as storage backend for a CernVM-FS repo.

## 1. Create new S3 Bucket

If not done so far, install `s3cmd`

```bash
    s3cmd --configure # create a new bucket

    # following parameters should be set
    Access Key: <access key>
    Secret Key: <secret key> 
    Default Region [US]: EU
    S3 Endpoint [s3.amazonaws.com]: https://s3.cern.ch
    # this here is the new bucket
    DNS-style bucket+hostname:port template for accessing a bucket [my-test-bucket]: my-test-bucket.cern.ch 
```

- In case there are problems with the location
  ```bash
    s3cmd --bucket-location=":default-placement" mb s3://my-test-bucket
  ```

- Try access the s3 bucket: `s3cmd ls s3://my-test-bucket`
- Config is saved in `/<user>/.s3cfg`

## 2. Create new CernVM-FS repository 

As for all `cvmfs_server` commands: `sudo systemctl stop autofs`

- New CernVM-FS repository name: `s3.test.repo`
- Use the just newly created S3 bucket `my-test-bucket.cern.ch`
 

### Setup S3 config for `cvmfs`
  - Might be easiest to make a new directory `/etc/cvmfs/s3config`

      - File: `/etc/cvmfs/s3config/s3.test.repo.cfg`
        ```
          CVMFS_S3_ACCESS_KEY=<access key>
          CVMFS_S3_SECRET_KEY=<secret key> 
          CVMFS_S3_HOST=s3.cern.ch
          CVMFS_S3_BUCKET=my-test-bucket
          CVMFS_S3_USE_HTTPS=YES
        ```

### Create repo `s3.test.repo` that uses S3 as storage backend

```bash
  cvmfs_server mkfs   -s /etc/cvmfs/s3config/s3.test.repo.cfg \
                      -w http://my-test-bucket.s3.cern.ch \
                      s3.test.repo
```

## 3. Test the repository

- Add some data with `cvmfs_server` 
  ```bash
    cvmfs_server transaction s3.test.repo
    cp -r /usr/bin/ /cvmfs/s3.test.repo/
    cvmfs_server publish s3.test.repo
  ```
    
- Check that data is actually there
  ```bash
    ls /cvmfs/s3.test.repo/bin/

    s3cmd ls s3://my-test-bucket/s3.test.repo/
    # output
    DIR  s3://my-test-bucket/s3.test.repo/data/
    2022-08-08 14:29           68  s3://my-test-bucket/s3.test.repo/.cvmfs_master_replica
    2022-08-08 14:35          594  s3://my-test-bucket/s3.test.repo/.cvmfspublished
    2022-08-08 14:35         5120  s3://my-test-bucket/s3.test.repo/.cvmfsreflog
    2022-08-08 14:29          405  s3://my-test-bucket/s3.test.repo/.cvmfswhitelist
  ```
    
