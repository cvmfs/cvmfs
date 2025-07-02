package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

func ReadChanges(file *os.File) chan string {

	changes := make(chan string)

	file.Seek(0, os.SEEK_END)
	bf := bufio.NewReader(file)

	go func() {
		for {
			line, _, err := bf.ReadLine()
			if len(line) != 0 {
				changes <- string(line)
			} else if err == io.EOF {
				time.Sleep(1 * time.Second)
			}
		}
	}()
	return changes
}

func ProcessRequest(logfile_name string, file_name string, repository_name string, rotation int) {

	file, err := os.OpenFile(file_name, os.O_RDONLY|os.O_CREATE, 0755)
	if err != nil {
		log.Fatalf("OpenFile: %s", err)
	}

	if rotation == 1 {
		// To make sure we haven't missed any image during rotation
		scanner := bufio.NewScanner(file)
		scanner.Split(bufio.ScanLines)
		for scanner.Scan() {
			ExecDucc(scanner.Text(), logfile_name, repository_name)
		}
		rotation = 0
	}

	changes := ReadChanges(file)

	for {

		msg := <-changes

		if msg == "xx|file rotation|xx" {
			file.Close()
			rotation = 1
			ProcessRequest(logfile_name, file_name, repository_name, rotation)
		}

		typeValue, err := checkImageType(msg)
		msg_split := strings.Split(msg, "|")
		action := msg_split[len(msg_split)-2]
		if action == "push" {
			if err != nil {
				log.Fatalf("Error checking image type querying harbor's API: %s", err)
			}
			if typeValue == "IMAGE" {
				ExecDucc(msg, logfile_name, repository_name)
			} else if typeValue == "SIF" {
				ExecSIF(msg, logfile_name, repository_name)
			}
		} else if action == "delete" {
			ExecDucc(msg, logfile_name, repository_name)
		}
	}
}

type Artifact struct {
	Type string `json:"type"`
}

func checkImageType(msg string) (string, error) {

	// Extract the image URL from the message
	parts := strings.Split(msg, "|")
	imageURL := parts[len(parts)-1]

	// Remove https:// and split into components
	image := strings.TrimPrefix(imageURL, "https://")
	segments := strings.Split(image, "/")
	if len(segments) < 3 {
		return "", fmt.Errorf("unexpected image URL format")
	}

	host := segments[0]
	project := segments[1]

	// Join the remaining segments (everything after host and project) using %252F
	pathParts := segments[2:]
	if len(pathParts) < 1 {
		return "", fmt.Errorf("repository part is missing")
	}
	// The last segment is expected to contain the tag (e.g., repo:tag)
	repoTag := pathParts[len(pathParts)-1]
	repoParts := strings.SplitN(repoTag, ":", 2)
	if len(repoParts) != 2 {
		return "", fmt.Errorf("unexpected repo format in image URL")
	}
	repoName := strings.Join(pathParts[:len(pathParts)-1], "%252F")
	repo := repoName
	if repo != "" {
		repo += "%252F" + repoParts[0]
	} else {
		repo = repoParts[0]
	}
	tag := repoParts[1]

	// Build API URL
	apiURL := fmt.Sprintf("https://%s/api/v2.0/projects/%s/repositories/%s/artifacts/%s", host, project, repo, tag)

	// Make HTTP GET request
	resp, err := http.Get(apiURL)
	if err != nil {
		return "", fmt.Errorf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// Decode JSON response
	var artifact Artifact
	if err := json.NewDecoder(resp.Body).Decode(&artifact); err != nil {
		return "", fmt.Errorf("failed to decode JSON: %v", err)
	}

	if artifact.Type == "" {
		return "", fmt.Errorf("'type' field not found or empty")
	}

	return artifact.Type, nil
}

func ExecSIF(msg string, logfile_name string, repository_name string) {

	msg_split := strings.Split(msg, "|")
	image := msg_split[len(msg_split)-1]
	image = strings.ReplaceAll(image, "https://", "")
	action := msg_split[len(msg_split)-2]
	ima_split := strings.Split(image, "/")
	dkrepo := ima_split[len(ima_split)-1]

	nOfE := 0
	repeat := true

	for repeat {
		nOfE++
		repeat = false
		currentTime := time.Now()
		timestamp := currentTime.Format("060102-150405")
		lf_name := logfile_name + "_" + dkrepo + "_" + timestamp
		logFile, err := os.OpenFile(lf_name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, os.ModePerm)
		if err != nil {
			fmt.Printf("Error opening log file: %v\n", err)
			return
		}
		defer logFile.Close()
		logger := log.New(logFile, "", log.LstdFlags)
		logger.Printf("[SIF conversion n.%d for %s started...]\n", nOfE, image)
		fmt.Printf("[SIF conversion n.%d for %s started...]\n", nOfE, image)
		_, traErr := exec.Command("sudo", "cvmfs_server", "transaction", repository_name).Output()
		if traErr != nil {
			log.Fatal(traErr)
		}
		p := "/cvmfs/" + repository_name + "/" + image
		parentDir := filepath.Dir(p)
		orasURI := "oras://" + image

		// Create the parent directory before building
		if _, statErr := os.Stat(parentDir); os.IsNotExist(statErr) {
			log.Printf("Path does not exist, create: %s\n", parentDir)
			if _, mkdErr := exec.Command("sudo", "mkdir", "-p", parentDir).Output(); mkdErr != nil {
				_, err = exec.Command("sudo", "cvmfs_server", "abort", "-f", repository_name).Output()
				log.Fatalf("Failed to create directory %s: %v", parentDir, mkdErr)
			}
		}

		_, buiErr := exec.Command("sudo", "apptainer", "build", "--force", "--sandbox", p, orasURI).Output()
		_, cleErr := exec.Command("sudo", "apptainer", "cache", "clean", "-f").Output()
		if buiErr != nil {
			_, err = exec.Command("sudo", "cvmfs_server", "abort", "-f", repository_name).Output()
			log.Fatal(buiErr)
		}
		pcat := p + "/.cvmfscatalog"
		_, cleErr = exec.Command("sudo", "touch", pcat).Output()
		if cleErr != nil {
			_, err = exec.Command("sudo", "cvmfs_server", "abort", "-f", repository_name).Output()
			log.Fatal(cleErr)
		}
		_, pubErr := exec.Command("sudo", "cvmfs_server", "publish", repository_name).Output()
		if pubErr != nil {
			_, err = exec.Command("sudo", "cvmfs_server", "abort", "-f", repository_name).Output()
			log.Fatal(pubErr)
		}
		// Open the JSON file for reading
		_, chmodErr := exec.Command("sudo", "chmod", "0755", lf_name).Output()
		if chmodErr != nil {
			fmt.Println("Error executing chmod:", chmodErr)
			return
		}
		//TODO: adapt to SIF image
		file, fileErr := os.Open(lf_name)
		if fileErr != nil {
			fmt.Println("Error opening file:", fileErr)
			return
		}
		defer file.Close()

		// Create a scanner to read the file line by line
		scanner := bufio.NewScanner(file)

		// Loop through each line in the file
		for scanner.Scan() {

			line := scanner.Text()
			// Parse the line as a JSON object
			var data map[string]interface{}
			if err := json.Unmarshal([]byte(line), &data); err != nil {
				fmt.Printf("Error parsing JSON: %v\n", err)
				continue
			}

			// Check if "status" is "error"
			status, exists := data["status"]
			if exists && status == "error" {
				// Perform some action when "status" is "error"
				if action == "push" {
					logger.Printf("[SIF conversion n.%d failed for layer %s]\n", nOfE, data["layer"])
					fmt.Printf("[SIF conversion n.%d failed for layer %s]\n", nOfE, data["layer"])
				}
				repeat = true
				break
			}
		}

		if err := scanner.Err(); err != nil {
			fmt.Println("Error reading file:", err)
		}
		logger.Printf("[SIF conversion n.%d completed 'ok']\n", nOfE)
		fmt.Printf("[SIF conversion n.%d completed 'ok']\n", nOfE)
	}
}

// TODO: we could instead use the cvmfs module from ducc
func DeletePathsInRepo(repository_name string, paths_to_delete []string) (err error) {
	_, err = exec.Command("sudo", "cvmfs_server", "transaction", repository_name).Output()
	if err != nil {
		log.Fatal(err)
	}
	for _, p := range paths_to_delete {
		if !strings.HasPrefix(p, "/cvmfs/") {
			_, err = exec.Command("sudo", "cvmfs_server", "abort", "-f", repository_name).Output()
			log.Fatalln("Refusing to remove path outside of /cvmfs: ", p)
		}
		// Check if the path exists
		if _, statErr := os.Stat(p); os.IsNotExist(statErr) {
			log.Printf("Path does not exist, skipping: %s\n", p)
			continue
		} else if statErr != nil {
			// Other error (e.g., permission denied), abort and report
			_, _ = exec.Command("sudo", "cvmfs_server", "abort", "-f", repository_name).Output()
			log.Fatalf("Error checking path %s: %v\n", p, statErr)
		}
		// Only delete if it exists
		_, rmErr := exec.Command("sudo", "rm", "-rf", p).Output()
		if rmErr != nil {
			_, err = exec.Command("sudo", "cvmfs_server", "abort", "-f", repository_name).Output()
			log.Fatal(rmErr)
		}
		parent := filepath.Dir(p)
		cvmfsRoot := "/cvmfs/" + repository_name
		for {
			if parent == cvmfsRoot || parent == "/" {
				break
			}
			// Check if the directory is empty
			entries, readErr := os.ReadDir(parent)
			if readErr != nil {
				log.Printf("Error reading directory %s: %v\n", parent, readErr)
				break
			}
			if len(entries) > 0 {
				break // Directory is not empty; stop cleaning up
			}
			// Attempt to remove the empty directory
			rmDirErr := exec.Command("sudo", "rmdir", parent).Run()
			if rmDirErr != nil {
				log.Printf("Could not remove directory %s: %v\n", parent, rmDirErr)
				break
			}
			// Move up to next parent
			parent = filepath.Dir(parent)
		}
	}

	_, pubErr := exec.Command("sudo", "cvmfs_server", "publish", repository_name).Output()
	if pubErr != nil {
		_, err = exec.Command("sudo", "cvmfs_server", "abort", "-f", repository_name).Output()
		log.Fatal(pubErr)
	}
	return nil
}

func ExecDucc(msg string, logfile_name string, repository_name string) {

	msg_split := strings.Split(msg, "|")
	image := msg_split[len(msg_split)-1]
	image = strings.ReplaceAll(image, "https://", "")
	action := msg_split[len(msg_split)-2]
	ima_split := strings.Split(image, "/")
	dkrepo := ima_split[len(ima_split)-1]

	nOfE := 0
	repeat := true

	for repeat {
		nOfE++
		repeat = false
		currentTime := time.Now()
		timestamp := currentTime.Format("060102-150405")
		lf_name := logfile_name + "_" + dkrepo + "_" + timestamp

		if action == "push" {
			fmt.Printf("[DUCC conversion n.%d for %s started...]\n", nOfE, image)
			_, err := exec.Command("sudo", "cvmfs_ducc", "convert-single-image", "-n", lf_name, "-p", image, repository_name, "--skip-thin-image", "--skip-podman").Output()
			if err != nil {
				log.Fatal(err)
			}
		} else if action == "delete" {
			fmt.Printf("[DUCC garbage collection n.%d for %s started...]\n", nOfE, image)
			image_path := "/cvmfs/" + repository_name + "/" + image
			image_manifest := "/cvmfs/" + repository_name + "/.metadata/" + image
			DeletePathsInRepo(repository_name, []string{image_path, image_manifest})
			_, gcErr := exec.Command("sudo", "cvmfs_ducc", "garbage-collection", "--grace-period", "0", "-n", lf_name, repository_name).Output()
			if gcErr != nil {
				log.Fatal(gcErr)
			}
		}
		// Open the JSON file for reading
		_, chmodErr := exec.Command("sudo", "chmod", "0755", lf_name).Output()
		if chmodErr != nil {
			fmt.Println("Error executing chmod:", chmodErr)
			return
		}

		file, fileErr := os.Open(lf_name)
		if fileErr != nil {
			fmt.Println("Error opening file:", fileErr)
			return
		}
		defer file.Close()

		// Create a scanner to read the file line by line
		scanner := bufio.NewScanner(file)

		// Loop through each line in the file
		for scanner.Scan() {

			line := scanner.Text()
			// Parse the line as a JSON object
			var data map[string]interface{}
			if err := json.Unmarshal([]byte(line), &data); err != nil {
				fmt.Printf("Error parsing JSON: %v\n", err)
				continue
			}

			// Check if "status" is "error"
			status, exists := data["status"]
			if exists && status == "error" {
				// Perform some action when "status" is "error"
				if action == "push" {
					fmt.Printf("[DUCC conversion n.%d failed for layer %s]\n", nOfE, data["layer"])
				} else if action == "delete" {
					fmt.Printf("[DUCC garbage collection n.%d failed]\n", nOfE)
				}
				repeat = true
				break
			}
		}

		if err := scanner.Err(); err != nil {
			fmt.Println("Error reading file:", err)
		}

	}

	if action == "push" {
		fmt.Printf("[DUCC conversion n.%d completed 'ok']\n", nOfE)
	} else if action == "delete" {
		fmt.Printf("[DUCC garbage collection n.%d completed 'ok']\n", nOfE)
	}
}

func main() {

	var rotation int

	logfile_name := flag.String("log_file", "ducc-conversion.log", "DUCC log file")
	file_name := flag.String("notifications_file", "notifications.txt", "Notification file")
	repository_name := flag.String("repository_name", "unpacked.cern.ch", "Repository")
	flag.Parse()

	lname := *logfile_name
	fname := *file_name
	rname := *repository_name

	ProcessRequest(lname, fname, rname, rotation)
}
