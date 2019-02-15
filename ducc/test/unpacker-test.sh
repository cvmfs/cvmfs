#!/bin/bash

registry=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 8 | head -n 1)
recipe="$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 8 | head -n 1).yaml"
repository="$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 8 | head -n 1).cern.ch"

clean_up() {
    printf "Cleaning up..."
    
    mv /etc/cvmfs/default.local.bk /etc/cvmfs/default.local
    rm -f /etc/cvmfs/config.d/$repository.conf
    #delete the docker images
    
    # delete the CVMFS repo
    printf "Deleting CVMFS repo..."
    cvmfs_server rmfs -f $repository >> /dev/null
    printf "done\n"
    
    export DOCKER2CVMFS_DOCKER_REGISTRY_PASS=""
    
    # delete the recipe file
    rm $recipe
    
    # stop and delete the registry container
    printf "Stopping and deleting docker registry..."
    docker stop $registry >> /dev/null
    docker rm $registry >> /dev/null
    printf "done\n"
    
    printf "Reseting initial docker configuration..."
    rm -f /etc/docker/daemon.json
    mv /etc/docker/daemon.json.bk /etc/docker/daemon.json
    
    systemctl restart docker
    
    printf "done\n"
}


main() {
    trap clean_up EXIT
    
    # change the docker configuration to run thin images
    printf "Changing the docker configuration to run thin images..."
    
    # saving old docker configuration
    mv /etc/docker/daemon.json /etc/docker/daemon.json.bk
    
    # installing thin image plugin
    docker plugin install --grant-all-permissions cvmfs/graphdriver
    
    # change configuration file
    cat > /etc/docker/daemon.json << EOL
{
  "experimental": true,
  "storage-driver": "cvmfs/graphdriver",
  "storage-opts": [
    "overlay2.override_kernel_check=true"
  ]
}
EOL
    
    # finally restarting docker
    systemctl restart docker
    printf "done\n"
    

    # start by running the docker registry on localhost
    printf "Starting docker registry for tests..."
    docker run -d -p 5000:5000 --name $registry registry:2 >> /dev/null || return 3
    printf " done\n"
    
    # create a simple recipe file for the repository manager in the local dir
    printf "Creating recipe file..."
    cat > $recipe  << EOL
version: 1
user: mock_user
cvmfs_repo: '$repository'
output_format: '\$(scheme)://localhost:5000/mock/\$(image)'
input:
    - 'https://registry.hub.docker.com/library/ubuntu:latest'
    - 'https://registry.hub.docker.com/library/centos:centos6'
EOL
    printf "done\n"
    
    # set the password to access the docker hub
    export DOCKER2CVMFS_DOCKER_REGISTRY_PASS=mock_pass
    
    # crete the repository where to store the content
    printf "Creating CVMFS repo..."
    cvmfs_server mkfs -o $USER $repository >> /dev/null
    printf "done\n"
    
    printf "Changing configuration to read the new repo..."
    # setting the configuration for the repo
    mv /etc/cvmfs/default.local /etc/cvmfs/default.local.bk

    cat > /etc/cvmfs/default.local << EOL
CVMFS_REPOSITORIES=$repository
CVMFS_HTTP_PROXY="DIRECT"
EOL

    cat > /etc/cvmfs/config.d/$repository.local << EOL
CVMFS_SERVER_URL=http://localhost/cvmfs/$repository/
CVMFS_PUBLIC_KEY=/etc/cvmfs/keys/$repository.pub
EOL
    printf "done\n"

    printf "Starting test.\n"
    
    printf "Converting recipe...\n"
    repository-manager convert $recipe || return 101
    printf "Convert recipe successfully\n"
    
    printf "Check integrity of the repository...\n"
    cvmfs_server check $repository || return 102
    printf "Repository checked successfully\n"
    
    singularity exec /cvmfs/$repository/registry.hub.docker.com/library/ubuntu\:latest/ echo token-abc | grep "token-abc" || return 103
    singularity exec /cvmfs/$repository/registry.hub.docker.com/library/centos\:centos6/ echo token-xyz | grep "token-xyz" || return 104

    docker run "localhost:5000/mock/library/ubuntu:latest" echo "token-123" | grep "token-123" || return 105
    docker run "localhost:5000/mock/library/centos:centos6" echo "token-321" | grep "token-321" || return 106
    
    # add other possible tests
        
    printf "\n\nTest successful\n\n"
    
    return
}

main

