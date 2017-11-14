package main

import (
	"fmt"
	"os"
	"os/exec"
	"path"
)

type CvmfsManager struct {
	fqrn string
}

func (cm CvmfsManager) StartTransaction() error {
	cmd := exec.Command("cvmfs_server", "transaction", publisherConfig.fqrn)
	if out, err := cmd.CombinedOutput(); err != nil {
		fmt.Println("ERROR: failed to open transaction!")
		fmt.Println(err)
		fmt.Println(out)
		return err
	}
	fmt.Println("Started transaction...")
	return nil
}

func (cm CvmfsManager) ImportTarball(src, digest string) error {
	dst := path.Join("/cvmfs", cm.fqrn, "layers", digest)
	fmt.Println("Import tarball destination: " + dst)

	if err := os.MkdirAll(dst, os.ModePerm); err != nil {
		fmt.Println("Failed to create destination!")
		fmt.Println(err)
		return err
	}
	tarCmd := fmt.Sprintf("bsdtar -xf %s -C %s", src, dst)

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
	cmd := exec.Command("cvmfs_server", "publish", publisherConfig.fqrn)
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

	out, err := exec.Command("cvmfs_server", "abort", "-f", publisherConfig.fqrn).CombinedOutput()
	if err != nil {
		fmt.Println(string(out))
		return err
	}

	return nil
}

func (cm CvmfsManager) LookupLayer(hash string) bool {
	dst := path.Join("/cvmfs", cm.fqrn, "layers", hash)
	fmt.Printf("Layer lookup path: %s\n", dst)

	if _, err := os.Stat(dst); err == nil {
		return true
	} else if os.IsNotExist(err) {
		return false
	} else {
		fmt.Println(err)
		return false
	}
}
