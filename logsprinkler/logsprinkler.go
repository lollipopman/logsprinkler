// vim: noexpandtab tabstop=2 shiftwidth=2:
// syslog sprinkler daemon
package main

import (
	"encoding/json"
	"fmt"
	"github.com/davecheney/profile"
	"github.com/jeromer/syslogparser"
	"github.com/lollipopman/syslogd"
	"log/syslog"
	"net"
	"os"
	"regexp"
	"time"
)

var logger *syslog.Writer

type ClientGroup struct {
	GroupRegexp regexp.Regexp
	Clients     map[string]ClientConfigMessage
}

type RemoteHeartbeatMessage struct {
	Type      string
	LogRegexp string
}

type ClientConfigMessage struct {
	LogRegexp      string
	NetworkAddress net.UDPAddr
	LastHeartbeat  time.Time
}

func init() {
	var err error
	logger, err = syslog.Dial("udp", "localhost:514", syslog.LOG_INFO|syslog.LOG_DAEMON, "logsprinkler")
	checkErrorFatal(err)
}

func handleClientHeartbeats(conn *net.UDPConn, clients chan ClientConfigMessage) {
	var remoteHeartbeatMessage RemoteHeartbeatMessage
	var clientConfigMessage ClientConfigMessage
	byteHeartbeatMessage := make([]byte, 1024)

	for {
		bytesRead, networkAddress, err := conn.ReadFromUDP(byteHeartbeatMessage)
		if checkError(err) {
			continue
		}
		err = json.Unmarshal(byteHeartbeatMessage[:bytesRead], &remoteHeartbeatMessage)
		if checkError(err) {
			continue
		}
		clientConfigMessage.NetworkAddress = *networkAddress
		clientConfigMessage.LastHeartbeat = time.Now()
		clientConfigMessage.LogRegexp = remoteHeartbeatMessage.LogRegexp
		clients <- clientConfigMessage
	}
}

func serveSyslogs(conn *net.UDPConn, clients chan ClientConfigMessage) {
	var err error
	var newClientGroup ClientGroup
	var newClientMap map[string]ClientConfigMessage
	syslogChannel := make(chan syslogparser.LogParts, 1)
	syslogServer := syslogd.NewServer()
	err = syslogServer.ListenUDP(":5515")
	checkErrorFatal(err)
	syslogServer.Start(syslogChannel)
	activeClients := make(map[string]ClientGroup)
	pruneDeadClientsTicker := time.NewTicker(time.Second * 10).C

	for {
		select {
		case clientConfigMessage := <-clients:
			if clientGroup, ok := activeClients[clientConfigMessage.LogRegexp]; ok {
				clientGroup.Clients[clientConfigMessage.NetworkAddress.String()] = clientConfigMessage
			} else {
				clientRegex, err := regexp.Compile(clientConfigMessage.LogRegexp)
				if checkError(err) {
					fmt.Print("New client with bad regexp")
				} else {
					newClientGroup.GroupRegexp = *clientRegex
					newClientMap = make(map[string]ClientConfigMessage)
					newClientGroup.Clients = newClientMap
					newClientGroup.Clients[clientConfigMessage.NetworkAddress.String()] = clientConfigMessage
					activeClients[clientConfigMessage.LogRegexp] = newClientGroup
				}
			}
		case <-pruneDeadClientsTicker:
			pruneDeadClients(activeClients)
			fmt.Print(len(activeClients))
		case logParts := <-syslogChannel:
			// Example rsyslog message: 'Apr  3 19:23:40 apply02 nginx-apply: this is the message'
			unformattedTime := logParts["timestamp"].(time.Time)
			formattedTime := unformattedTime.Format(time.Stamp)
			logMessage := fmt.Sprintf("%s %s %s%s %s\n", formattedTime, logParts["hostname"], logParts["tag"], ":", logParts["content"])
			logTag := fmt.Sprintf("%s", logParts["tag"])
			for _, clientGroup := range activeClients {
				if len(clientGroup.Clients) > 0 && clientGroup.GroupRegexp.MatchString(logTag) {
					for _, clientConfigMessage := range clientGroup.Clients {
						conn.WriteToUDP([]byte(logMessage), &clientConfigMessage.NetworkAddress)
					}
				}
			}
		}
	}
}

func pruneDeadClients(activeClients map[string]ClientGroup) {
	for _, clientGroup := range activeClients {
		for clientNetworkAddress, clientConfigMessage := range clientGroup.Clients {
			lastHeartbeatDuration := time.Now().Sub(clientConfigMessage.LastHeartbeat)
			if lastHeartbeatDuration.Seconds() >= 10 {
				delete(clientGroup.Clients, clientNetworkAddress)
			}
		}
	}
}

func checkErrorFatal(err error) {
	if err != nil {
		logger.Crit("Fatal error " + err.Error())
		os.Exit(1)
	}
}

func checkError(err error) bool {
	foundError := false
	if err != nil {
		foundError = true
		logger.Warning("Error " + err.Error())
	}
	return foundError
}

func main() {
	defer profile.Start(profile.CPUProfile).Stop()
	logger.Info("logsprinkler starting up")
	UDPAddr := net.UDPAddr{Port: 5514}
	conn, err := net.ListenUDP("udp", &UDPAddr)
	checkErrorFatal(err)
	clients := make(chan ClientConfigMessage)
	go handleClientHeartbeats(conn, clients)
	serveSyslogs(conn, clients)
}
