// vim: noexpandtab tabstop=2 shiftwidth=2:
// logbarrel: syslog sprinkler client

package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

func init() {
	// Output to stdout instead of the default stderr, could also be a file.
	log.SetOutput(os.Stderr)

	// Only log the warning severity or above.
	log.SetLevel(log.DebugLevel)
}

type RemoteHeartbeatMessage struct {
	Type       string
	Pid        int
	SyslogTags []string
}

func readTag(tag string, stdout chan string) {
	fileName := "./tags/" + tag + "/" + strconv.Itoa(os.Getpid())
	// TODO remove hack, of waiting for logsprinkler to open the file
	time.Sleep(1 * time.Second)
	file, err := os.OpenFile(fileName, os.O_RDONLY, os.ModeNamedPipe)
	checkError(err)

	scanner := bufio.NewScanner(file)
	log.Debugf("Scanning started for file %s", fileName)
	for scanner.Scan() {
		stdout <- scanner.Text()
	}
	if err := scanner.Err(); err != nil {
		log.Fatalf("Fatal error reading file", err.Error())
	}
	log.Debugf("Scanning complete")
}

func printStdout(stdout <-chan string) {
	for line := range stdout {
		fmt.Fprintln(os.Stdout, line)
	}
}

func sendHeartbeats(conn net.Conn, tags []string) {
	var remoteHeartbeatMessage RemoteHeartbeatMessage
	remoteHeartbeatMessage.Type = "heartbeat"
	remoteHeartbeatMessage.SyslogTags = tags
	remoteHeartbeatMessage.Pid = os.Getpid()
	byteHeartbeatMessage, err := json.Marshal(remoteHeartbeatMessage)
	checkError(err)

	for {
		_, err := conn.Write(byteHeartbeatMessage)
		log.Debug("Heartbeat sent")
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
	var tagsString = flag.String("t", "*", "Comma separated list of Syslog tags or '*' to select all")
	var tags []string

	flag.Parse()
	tags = strings.Split(*tagsString, ",")

	UDPAddr := net.UDPAddr{Port: 5514}
	conn, err := net.DialUDP("udp", nil, &UDPAddr)
	checkError(err)

	go sendHeartbeats(conn, tags)
	stdout := make(chan string)
	for tag := range tags {
		go readTag(tags[tag], stdout)
	}
	printStdout(stdout)
}
