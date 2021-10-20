package main

import (
  "bufio"
  "fmt"
  "io"
  "os"
  "log"
  "time"
  "flag"
  "strings"
  "os/exec"
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

func ProcessRequest(file_name string, repository_name string, rotation int) {

    file, err := os.OpenFile(file_name, os.O_RDONLY, 0755)
    if err != nil {
        log.Fatalf("OpenFile: %s", err)
        }

    if rotation == 1 {
	// To make sure we haven't miss any image during rotation
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
	    ExecDucc(scanner.Text(), repository_name)
	}
	rotation = 0
    }

    changes := ReadChanges(file)

    for {

        msg := <-changes

        if msg == "xx|file rotation|xx" {
		file.Close()
		rotation = 1
		ProcessRequest(file_name, repository_name, rotation)
        }

        ExecDucc(msg, repository_name)
    }
}

func ExecDucc(msg string, repository_name string) {

    msg_split := strings.Split(msg, "|")
    image := msg_split[len(msg_split)-1]
    image = strings.ReplaceAll(image, "https://", "")

    fmt.Printf("DUCC ingestion for %s started...\n", image)

    _, err := exec.Command("cvmfs_ducc", "convert-single-image", "-p", image, repository_name, "--skip-thin-image", "--skip-podman").Output()
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("[done]\n")
}

func main() {

    var rotation int

    file_name := flag.String("notifications_file", "notifications.txt", "Notification file")
    repository_name := flag.String("repository_name", "test-unpacked.cern.ch", "Repository")
    flag.Parse()

    fname := *file_name
    rname := *repository_name

    ProcessRequest(fname, rname, rotation)
}
