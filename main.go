package main

import (
  "io"
  "log"
  "net"
  "fmt"
  "time"
)

func main() {
  // Listen on TCP port 2000 on all interfaces.
  l, err := net.Listen("tcp", ":2000")
  if err != nil {
    log.Fatal(err)
  }
  defer l.Close()

  log_channel := make(chan string)
  new_client_channel := make(chan chan string)
  go func(new_client_channel chan chan string, log_channel chan string) {
    l, err := net.Listen("tcp", ":2001")
    if err != nil {
      log.Fatal(err)
    }
    defer l.Close()
    conn, err := l.Accept()

    var client_channel_list []chan string
    var client_channel chan string
    for {
      select {
        case client_channel = <-new_client_channel:
          client_channel_list = append(client_channel_list, client_channel)
          fmt.Println("NEW CLIENT")
        default: fmt.Println("NO NEW CLIENTS")
      }

      buf := make([]byte, 1024)
      reqLen, err := io.ReadFull(conn, buf)
      if err != nil && reqLen == 1024 {
        log.Fatal(err)
      }

      for _, client_channel = range client_channel_list {
        client_channel <- string(buf)
      }
      time.Sleep(2 * time.Second)
    }
  }(new_client_channel, log_channel)

  for {
    // Wait for a connection.
    conn, err := l.Accept()
    if err != nil {
      log.Fatal(err)
    }
    client_log_channel := make(chan string)
    new_client_channel <- client_log_channel
    // Handle the connection in a new goroutine.
    // The loop then returns to accepting, so that
    // multiple connections may be served concurrently.
    go func(c net.Conn, client_log_channel chan string) {
      // Echo all incoming data.
      for {
        io.WriteString(c, <-client_log_channel)
      }
      // Shut down the connection.
      c.Close()
    }(conn, client_log_channel)
  }
}
