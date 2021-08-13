package main

import (
  "bufio"
  "fmt"
  "io"
  "os"
  "log"
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
                time.Sleep(1 * time.Millisecond)
            }
        }
    }()
    return changes
}

func main() {

    file_name := "notifications.txt"
    file, err := os.OpenFile(file_name, os.O_RDONLY, 0755)
    if err != nil {
        log.Fatalf("openFile: %s", err)
        }

    changes := ReadChanges(file)

    for {
        msg := <-changes
	if msg == "xx|file rotation|xx" {
	    main()
	}
        fmt.Println(msg)
    }
}
