# Running tests in Containers

1. Setup
```sh
git clone https://github.com/cvmfs/cvmfs.git
cd tests/common/container
docker-compose up --build -d
```

2. Build:
```sh
docker exec -u sftnight -it cvmfs-dev bash
cmake -S /home/sftnight/cvmfs -B /tmp/cvmfs-build -D EXTERNALS_PREFIX=/tmp/cvmfs-ext -D BUILD_SHRINKWRAP=ON
cd /tmp/cvmfs-build
make 
sudo make install
```

3. Setup CVMFS
```sh
sudo cvmfs_config setup
```

4. Testing
```sh
cd /home/sftnight/cvmfs/test/common/container
bash test.sh
```

