// vim: noexpandtab tabstop=2 shiftwidth=2:
package main

import (
	"fmt"
	"github.com/jeromer/syslogparser"
	"github.com/wolfeidau/syslogasuarus/syslogd"
	"io"
	"log"
	"net"
)

func main() {
	l, err := net.Listen("tcp", ":5514")
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	log_channel := make(chan string)
	new_client_channel := make(chan chan string)
	go func(new_client_channel chan chan string, log_channel chan string) {
		syslog_channel := make(chan syslogparser.LogParts, 1)
		svr := syslogd.NewServer()
		svr.ListenUDP(":5514")
		svr.Start(syslog_channel)

		var client_channel_list []chan string
		var client_channel chan string
		for {
			select {
			case client_channel = <-new_client_channel:
				client_channel_list = append(client_channel_list, client_channel)
				fmt.Println("New client")
			case logParts := <-syslog_channel:
				log_message := fmt.Sprintf("%s %s %s %s %s\n", logParts["timestamp"], logParts["severity"], logParts["hostname"], logParts["tag"], logParts["content"])
				fmt.Println(log_message)
				for _, client_channel = range client_channel_list {
					client_channel <- log_message
				}
			}
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
