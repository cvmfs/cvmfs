#!/bin/sh

# this script creates a new user which will be used formar
# running cvmfs in the default case. (default name: cvmfs) 

username=cvmfs

# check if user already exists
testun=`dscl . -list /Users | grep $username`
if [ "$testun" == "$username" ]; then
  echo "User already existed"
  exit 0;
fi

# Find out the next available user ID
maxID=$(dscl . -list /Users UniqueID | awk '{print $2}' | sort -ug | tail -1)
userID=$((maxID+1))

# create a user account for cvmfs
dscl . -create /Users/$username
dscl . -create /Users/$username RealName "CernVM-FS user"
dscl . -create /Users/$username UserShell /bin/sh
dscl . -passwd /Users/$username \*
dscl . -create /Users/$username UniqueID $userID
dscl . -create /Users/$username PrimaryGroupID 20

# hide the user from the login prompt
defaults write /Library/Preferences/com.apple.loginwindow Hide500Users -bool TRUE
defaults write /Library/Preferences/com.apple.loginwindow HiddenUsersList -array $username