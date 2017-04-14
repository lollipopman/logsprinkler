// vim: noexpandtab tabstop=2 shiftwidth=2:
// logbarrel: syslog sprinkler client

package main

import (
	"encoding/json"
	"os"
	"flag"
	"fmt"
	"log"
	"net"
	"strings"
	"time"
)

type RemoteHeartbeatMessage struct {
	Type       string
	Pid        int
	SyslogTags []string
}

func receiveSyslogs(conn net.Conn) {
	readBuffer := make([]byte, 1024)
	for {
		bytesRead, err := conn.Read(readBuffer[0:])
		checkError(err)
		fmt.Print(string(readBuffer[0:bytesRead]))
	}
}

func sendHeartbeats(conn net.Conn, syslogTags []string) {
	var remoteHeartbeatMessage RemoteHeartbeatMessage
	remoteHeartbeatMessage.Type = "heartbeat"
	remoteHeartbeatMessage.SyslogTags = syslogTags
	remoteHeartbeatMessage.Pid = os.Getpid()
	byteHeartbeatMessage, err := json.Marshal(remoteHeartbeatMessage)
	checkError(err)

	for {
		_, err := conn.Write(byteHeartbeatMessage)
		checkError(err)
		time.Sleep(2 * time.Second)
	}
}

func checkError(err error) {
	if err != nil {
		log.Fatalf("Fatal error ", err.Error())
	}
}

func main() {
	var syslogTagsString = flag.String("t", "*", "Comma separated list of Syslog tags or '*' to select all")
	var syslogTags []string

	flag.Parse()
	syslogTags = strings.Split(*syslogTagsString, ",")

	UDPAddr := net.UDPAddr{Port: 5514}
	conn, err := net.DialUDP("udp", nil, &UDPAddr)
	checkError(err)

	go sendHeartbeats(conn, syslogTags)
	receiveSyslogs(conn)
}
