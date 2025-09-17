package main

import "github.com/cvmfs/cvmfs/cvmfs-posix-tools/pkg"

var purge = func(ctx Context, db pkg.DB) error {

	s3Interface, err := newBasicS3Manager(ctx)
	if err != nil {
		return err
	}
	alternateS3Interface, err := newAlternateS3Manager(ctx)
	if err != nil {
		return err
	}
	err = purgeFiles(ctx, db, s3Interface, alternateS3Interface)
	if err != nil {
		return err
	}

	return nil

}
