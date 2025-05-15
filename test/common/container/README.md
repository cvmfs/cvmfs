# Running tests in Containers

1. Setup and build
```sh
git clone https://github.com/cvmfs/cvmfs.git
cd tests/common/container
docker-compose up --build -d
```

2. Setup CVMFS:
```sh
docker exec -u sftnight -it cvmfs-dev bash
sudo cvmfs_config setup
```

3. Testing
```sh
cd /home/sftnight/cvmfs/test/common/container
bash test.sh
```

