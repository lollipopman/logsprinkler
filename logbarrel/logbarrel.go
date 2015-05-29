// vim: noexpandtab tabstop=2 shiftwidth=2:
// logbarrel: syslog sprinkler client

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"regexp"
	"time"
)

type RemoteHeartbeatMessage struct {
	Type      string
	LogRegexp string
}

func receiveSyslogs(conn net.Conn) {
	readBuffer := make([]byte, 1024)
	for {
		bytesRead, err := conn.Read(readBuffer[0:])
		checkError(err)
		fmt.Print(string(readBuffer[0:bytesRead]))
	}
}

func sendHeartbeats(conn net.Conn, logRexp *regexp.Regexp) {
	var remoteHeartbeatMessage RemoteHeartbeatMessage
	remoteHeartbeatMessage.Type = "heartbeat"
	remoteHeartbeatMessage.LogRegexp = logRexp.String()
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
	var logRegexpArg = flag.String("tag-regexp", ".*", "Receive syslog tags matching this regular expression")
	flag.Parse()
	logRegexp, err := regexp.Compile(*logRegexpArg)
	checkError(err)
	UDPAddr := net.UDPAddr{Port: 5514}
	conn, err := net.DialUDP("udp", nil, &UDPAddr)
	checkError(err)
	go sendHeartbeats(conn, logRegexp)
	receiveSyslogs(conn)
}
