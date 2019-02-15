#!/bin/bash

error_count=0

while true
do
        
        
        git --version > /dev/null 2>&1 || { echo "git is required but it is not installed. Aborting."; exit 1; }
        ducc version > /dev/null 2>&1 || { echo "ducc is required but it is not installed. Aborting."; exit 1; }

	update_error_count=0
	while true
	do

		# if for 3 times was no possible to update the repository we send an email and we exit.
		if [ $update_error_count -ge 3 ]; then
			echo "" | mail -s "[unpacked.cern.ch] Error on contacting the gitlab repository" -t simone.mosciatti@cern.ch
			exit 2;
		fi

	        (git clone ssh://git@gitlab.cern.ch:7999/unpacked/sync.git unpacked > /dev/null)

	        pushd unpacked
	        (git pull origin master)
	        pull_status=$?
		popd

		if [ $pull_status -eq 0 ]; then
			break;
		else
			update_error_count=$(( update_error_count+1))
		fi
	done

        (ducc convert unpacked/recipe.yaml)
        conversion_status=$?
        
        echo "ReturnCode: $conversion_status"
        if [ $conversion_status -ne 0 ]; then
                error_count=$(( error_count+1 ))
                echo "Error: $conversion_status"
                echo "error_count: $error_count"
                if [ $error_count -ge 10 ]; then
                        echo "" | mail -s "[unpacked.cern.ch] Error on unpacked.cern.ch $conversion_status" -t simone.mosciatti@cern.ch
                        exit 1
                fi
        else
                echo "Re-setting error_count"
                error_count=0
        fi
        sleep 10
done
