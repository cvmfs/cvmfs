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
  "encoding/json"
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

        ExecDucc(msg, logfile_name, repository_name)
    }
}

func ExecDucc(msg string, logfile_name string, repository_name string) {

    msg_split := strings.Split(msg, "|")
    image := msg_split[len(msg_split)-1]
    image = strings.ReplaceAll(image, "https://", "")
    action := msg_split[len(msg_split)-2]
    ima_split := strings.Split(image, "/")
    dkrepo := ima_split[len(ima_split)-1]

    if action == "push" {

        nOfE := 0
        repeat := true
        lf_name := ""

        for repeat {
          nOfE++
          repeat = false
          currentTime := time.Now()
          timestamp := currentTime.Format("060102-150405")
          lf_name = logfile_name + "_" + dkrepo + "_" + timestamp
          fmt.Printf("[DUCC conversion n.%d for %s started...]\n", nOfE, image)

          _, err := exec.Command("sudo", "cvmfs_ducc", "convert-single-image", "-n", lf_name, "-p", image, repository_name, "--skip-thin-image", "--skip-podman").Output()
          if err != nil {
            log.Fatal(err)
          }

          // Open the JSON file for reading
          _, chmodErr := exec.Command("sudo", "chmod", "0755", lf_name).Output()
          if chmodErr != nil {
                fmt.Println("Error executing chmod:", chmodErr)
                return
          }

          file, fileErr := os.Open(lf_name)
          if err != nil {
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
                        fmt.Printf("[DUCC conversion n.%d failed for layer %s]\n", nOfE, data["layer"])
                        repeat = true
                        break;
                }
          }

          if err := scanner.Err(); err != nil {
                fmt.Println("Error reading file:", err)
          }

        }

        fmt.Printf("[DUCC conversion n.%d completed 'ok']\n", nOfE)
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
