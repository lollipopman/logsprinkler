// vim: noexpandtab tabstop=2 shiftwidth=2:

// syslog sprinkler daemon
package main

import (
	"fmt"
	"github.com/jeromer/syslogparser"
	"github.com/lollipopman/syslogd"
	"log/syslog"
	"net"
	"os"
	"time"
)

var logger *syslog.Writer

func init() {
		var err error
		logger, err = syslog.Dial("udp", "localhost:514", syslog.LOG_INFO|syslog.LOG_DAEMON, "syslog-sprinkler")
		checkErrorFatal(err)
}

func handleClientHeartbeats(conn *net.UDPConn, clients chan string) {

	var buf [1024]byte

	for {
		n, addr, err := conn.ReadFromUDP(buf[0:])
		if err != nil {
			logger.Info("Error: Received bad UDP packet\n")
		} else if string(buf[0:n]) != "HEARTBEAT" {
			logger.Info("Error: Received packet without a heatbeat message" + string(buf[0:n]) + "\n")
		} else {
			clients <- addr.String()
		}
	}
}

func serveSyslogs(conn *net.UDPConn, clients chan string) {
	var err error
	syslogChannel := make(chan syslogparser.LogParts, 1)
	syslogServer := syslogd.NewServer()
	err = syslogServer.ListenUDP(":5515")
	checkErrorFatal(err)
	syslogServer.Start(syslogChannel)

	activeClients := make(map[string]time.Time)

	for {
		select {
		case clientAddr := <-clients:
			activeClients[clientAddr] = time.Now()
		case logParts := <-syslogChannel:
			pruneDeadClients(activeClients)

			// Example rsyslog message: 'Apr  3 19:23:40 apply02 nginx-apply: this is the message'
			unformattedTime := logParts["timestamp"].(time.Time)
			formattedTime := unformattedTime.Format(time.Stamp)
			logMessage := fmt.Sprintf("%s %s %s%s %s\n", formattedTime, logParts["hostname"], logParts["tag"], ":", logParts["content"])
			for clientAddr, _ := range activeClients {
				udpAddr, _ := net.ResolveUDPAddr("udp4", clientAddr)
				conn.WriteToUDP([]byte(logMessage), udpAddr)
			}
		}
	}
}

func pruneDeadClients(activeClients map[string]time.Time) {
	for clientAddr, lastHeartbeat := range activeClients {
		now := time.Now()
		lastHeartbeatDuration := now.Sub(lastHeartbeat)
		if lastHeartbeatDuration.Seconds() >= 10 {
			delete(activeClients, clientAddr)
		}
	}
}

func checkErrorFatal(err error) {
	if err != nil {
		logger.Crit("Fatal error " + err.Error())
		os.Exit(1)
	}
}

func main() {
	logger.Info("syslog-sprinkler starting up")
	service := ":5514"
	udpAddr, err := net.ResolveUDPAddr("udp4", service)
	checkErrorFatal(err)

	conn, err := net.ListenUDP("udp", udpAddr)
	checkErrorFatal(err)

	clients := make(chan string)
	go handleClientHeartbeats(conn, clients)
	serveSyslogs(conn, clients)
}
