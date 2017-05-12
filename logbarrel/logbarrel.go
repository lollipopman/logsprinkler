// vim: noexpandtab tabstop=2 shiftwidth=2:
// logbarrel: syslog sprinkler client

package main

import (
	"encoding/json"
	"flag"
	log "github.com/Sirupsen/logrus"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"
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

func readTag(tag string, stdout chan []byte) {
	fileName := "./tags/" + tag + "/" + strconv.Itoa(os.Getpid())
	time.Sleep(1 * time.Second)
	file, err := os.OpenFile(fileName, os.O_RDONLY|syscall.O_NONBLOCK, os.ModeNamedPipe)
	checkError(err)

	for {
		readBuffer := make([]byte, 1024)
		_, err := file.Read(readBuffer[0:])
		if err != nil {
			if err != io.EOF {
				//log.Info(err.Error())
			}
		} else {
			log.Info("BYTES")
			stdout <- readBuffer
		}
	}
}

func printStdout(stdout <-chan []byte) {
	for bytes := range stdout {
		os.Stdout.Write(bytes)
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
	stdout := make(chan []byte)
	for tag := range tags {
		go readTag(tags[tag], stdout)
	}
	printStdout(stdout)
}
