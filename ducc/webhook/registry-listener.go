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
                time.Sleep(1 * time.Millisecond)
            }
        }
    }()
    return changes
}

func NotifyDucc(file_name string, repository_name string) {

    file, err := os.OpenFile(file_name, os.O_RDONLY, 0755)
    if err != nil {
        log.Fatalf("OpenFile: %s", err)
        }

    changes := ReadChanges(file)

    for {
        msg := <-changes

        if msg == "xx|file rotation|xx" {
                NotifyDucc(file_name, repository_name)
        }


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
}

func main() {

    file_name := flag.String("notifications_file", "notifications.txt", "Notification file")
    repository_name := flag.String("repository_name", "test-unpacked.cern.ch", "Repository")
    flag.Parse()

    name := *file_name
    repo := *repository_name

    NotifyDucc(name, repo)
}
