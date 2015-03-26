package main

import (
  "io"
  "log"
  "net"
  "fmt"
  "gopkg.in/mcuadros/go-syslog.v2"
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

    syslog_channel := make(syslog.LogPartsChannel)
    handler := syslog.NewChannelHandler(syslog_channel)

    server := syslog.NewServer()
    server.SetFormat(syslog.RFC3164)
    server.SetHandler(handler)
    server.ListenTCP("0.0.0.0:5514")
    server.Boot()

    var client_channel_list []chan string
    var client_channel chan string
    for {
      select {
        case client_channel = <-new_client_channel:
          client_channel_list = append(client_channel_list, client_channel)
          fmt.Println("New client")
        case logParts := <-syslog_channel:
          for _, client_channel = range client_channel_list {
            if str, ok := logParts["content"].(string); ok {
              client_channel <- str + "\n"
            } else {
              fmt.Println(str)
              fmt.Println("Not a string!!!!")
            }
          }
      }
    }
    server.Wait()
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

