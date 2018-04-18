#!/bin/sh

if [ -f /etc/redhat-release ]; then
  printf $(cat /etc/redhat-release | grep -o -E '[0-9].[0-9]+')
else
  printf NOT_REDHAT
fi
