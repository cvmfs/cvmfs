#!/bin/sh

git commit -a -m changelog
git checkout devel
git merge --no-ff -n "Merge fix-changelog into devel" fix-changelog
git branch -d fix-changelog

