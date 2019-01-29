#!/bin/bash

error_count=0

while true
do
        
        
        git --version > /dev/null 2>&1 || { echo "git is required but it is not installed. Aborting."; exit 1; }
        ducc version > /dev/null 2>&1 || { echo "ducc is required but it is not installed. Aborting."; exit 1; }
        
        git clone ssh://git@gitlab.cern.ch:7999/unpacked/sync.git unpacked > /dev/null
        pushd unpacked
        git pull origin master || { echo "error in pulling the repository"; exit 1;}
        popd


        (ducc convert unpacked/recipe.yaml)
        conversion_status=$?
        
        echo "ReturnCode: $conversion_status"
        if [ $conversion_status -ne 0 ]; then
                error_count=$(( error_count+1 ))
                echo "Error: $conversion_status"
                echo "error_count: $error_count"
                if [ $error_count -ge 10 ]; then
                        echo "" | mail -s "Error on unpacked.cern.ch $conversion_status" -t simone.mosciatti@cern.ch
                        exit 1
                fi
        else
                echo "Re-setting error_count"
                error_count=0
        fi
        sleep 10
done
