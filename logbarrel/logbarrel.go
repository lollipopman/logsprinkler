// vim: noexpandtab tabstop=2 shiftwidth=2:
// logbarrel: syslog sprinkler client

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"runtime"
	"strings"
	"time"
)

// version set from linker
var version string

type RemoteHeartbeatMessage struct {
	Type       string
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
	var serverIPString = flag.String("s", "localhost", "Hostname or IP of server")
	var showVersion = flag.Bool("v", false, "Show version")
	var syslogTags []string

	flag.Parse()
	if *showVersion {
		fmt.Printf("logbarrel verision: %s, go runtime version: %s\n", version, runtime.Version())
		os.Exit(0)
	}
	syslogTags = strings.Split(*syslogTagsString, ",")
	serverIP, err := net.ResolveIPAddr("ip", *serverIPString)
	checkError(err)

	UDPAddr := net.UDPAddr{IP: serverIP.IP, Port: 5514}
	conn, err := net.DialUDP("udp", nil, &UDPAddr)
	checkError(err)

	go sendHeartbeats(conn, syslogTags)
	receiveSyslogs(conn)
}
