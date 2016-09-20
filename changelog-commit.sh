#!/bin/sh

git commit -a -m changelog
git checkout devel
git merge --no-ff -m "Merge fix-changelog into devel" fix-changelog
git branch -d fix-changelog

