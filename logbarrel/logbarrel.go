// vim: noexpandtab tabstop=2 shiftwidth=2:
// logbarrel: syslog sprinkler client

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"runtime"
	"runtime/pprof"
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
	readBuffer := make([]byte, 65507)
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
		log.Fatalf("Fatal error: %s", err.Error())
	}
}

func checkErrorFatal(err error) {
	if err != nil {
		log.Fatalf("Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

func main() {
	var syslogTagsString = flag.String("t", "*", "Comma separated list of Syslog tags or '*' to select all")
	var serverIPString = flag.String("s", "localhost", "Hostname or IP of server")
	var showVersion = flag.Bool("v", false, "Show version")
	var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
	var memprofile = flag.String("memprofile", "", "write memory profile to this file")
	var syslogTags []string

	flag.Parse()
	if *showVersion {
		fmt.Printf("logbarrel verision: %s, go runtime version: %s\n", version, runtime.Version())
		os.Exit(0)
	}
	syslogTags = strings.Split(*syslogTagsString, ",")
	serverIP, err := net.ResolveIPAddr("ip", *serverIPString)
	checkError(err)

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		checkErrorFatal(err)
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	UDPAddr := net.UDPAddr{IP: serverIP.IP, Port: 5514}
	conn, err := net.DialUDP("udp", nil, &UDPAddr)
	checkError(err)

	go sendHeartbeats(conn, syslogTags)
	receiveSyslogs(conn)

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		checkErrorFatal(err)
		pprof.Lookup("heap").WriteTo(f, 0)
		f.Close()
	}
}
