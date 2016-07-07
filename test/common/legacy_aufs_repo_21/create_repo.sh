#!/bin/sh

# This is meant as a blueprint to recreate the repository kept here as a tar.gz

set -e

repo=legacy_aufs.cern.ch
user=vagrant

sudo cvmfs_server mkfs -o $user $repo
cvmfs_server transaction $repo

cd /cvmfs/$repo

[ ! -f new_repository ] || rm -f new_repository

mkdir -p needs_migration/doesnt_need_migration
mkdir -p doesnt_need_migration/doesnt_need_migration_either
mkdir -p needs_commit_due_to_child/needs_migration

echo "foo" > foo
echo "bar" > bar
echo "baz" > baz
ln foo hardlink_foo
ln bar hardlink_bar
ln baz hardlink_baz

cd needs_migration
echo "foo" > foo
echo "bar" > bar
echo "baz" > baz
ln -s foo         symlink_foo
ln    bar         hardlink_bar
ln    symlink_foo hardlink_symlink_foo_1
ln    symlink_foo hardlink_symlink_foo_2
cp foo bar baz doesnt_need_migration
mkdir simple_directory
cp foo bar baz simple_directory
ln -s foo simple_directory/symlink_foo
ln    simple_directory/foo simple_directory/hardlink_foo
touch doesnt_need_migration/.cvmfscatalog
touch .cvmfscatalog

cd /cvmfs/$repo

cd doesnt_need_migration
echo "foo" > foo
echo "bar" > bar
echo "baz" > baz
ln -s foo symlink_foo
ln -s bar symlink_bar
cp foo bar baz symlink_foo doesnt_need_migration_either
mkdir simple_directory
cp foo bar baz simple_directory
ln -s foo simple_directory/symlink_foo
touch doesnt_need_migration_either/.cvmfscatalog
touch .cvmfscatalog

cd /cvmfs/$repo

cd needs_commit_due_to_child
echo "foo" > foo
echo "bar" > bar
echo "baz" > baz
ln -s baz symlink_baz
cp foo bar baz needs_migration
ln needs_migration/foo needs_migration/hardlink_foo
ln needs_migration/bar needs_migration/hardlink_bar_1
ln needs_migration/bar needs_migration/hardlink_bar_2
mkdir simple_directory
cp foo bar baz simple_directory
ln -s foo simple_directory/symlink_foo
touch needs_migration/.cvmfscatalog
touch .cvmfscatalog

cd /

cvmfs_server publish -v $repo
