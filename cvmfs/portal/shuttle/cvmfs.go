package main

import (
	"fmt"
	"os"
	"os/exec"
	"path"
)

type CvmfsManager struct {
	fqrn string
	cvmfsPathPrefix string
}

func (cm CvmfsManager) StartTransaction() error {
	cmd := exec.Command("cvmfs_server", "transaction", publisherConfig.Fqrn)
	if out, err := cmd.CombinedOutput(); err != nil {
		fmt.Println("ERROR: failed to open transaction!")
		fmt.Println(err)
		fmt.Println(out)
		return err
	}
	fmt.Println("Started transaction...")
	return nil
}

func (cm CvmfsManager) ImportTarball(src string) error {
	dst := path.Join("/cvmfs", cm.fqrn, cm.cvmfsPathPrefix)
	fmt.Println("Import tarball destination: " + dst)

	if err := os.MkdirAll(dst, os.ModePerm); err != nil {
		fmt.Println("Failed to create destination!")
		fmt.Println(err)
		return err
	}
	tarCmd := fmt.Sprintf("tar -xf %s -C %s", src, dst)

	cmd := exec.Command("bash", "-c", tarCmd)

	if out, err := cmd.CombinedOutput(); err != nil {
		fmt.Println("ERROR: failed to extract!")
		fmt.Printf("Command was: %s\n", tarCmd)
		fmt.Println(err)
		fmt.Println(string(out))
		return err
	}
	fmt.Println("Extracted")
	return nil
}

func (cm CvmfsManager) PublishTransaction() error {
	cmd := exec.Command("cvmfs_server", "publish", publisherConfig.Fqrn)
	if out, err := cmd.CombinedOutput(); err != nil {
		fmt.Println("ERROR: failed to publish!")
		fmt.Println(err)
		fmt.Println(string(out))
		return err
	} else {
		fmt.Println("Published transaction!")
		return nil
	}
}

func (cm CvmfsManager) AbortTransaction() error {
	fmt.Println("Aborting transaction!")

	out, err := exec.Command("cvmfs_server", "abort", "-f", publisherConfig.Fqrn).CombinedOutput()
	if err != nil {
		fmt.Println(string(out))
		return err
	}

	return nil
}
