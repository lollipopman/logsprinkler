// vim: noexpandtab tabstop=2 shiftwidth=2:
// syslog sprinkler daemon
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/jeromer/syslogparser"
	"github.com/lollipopman/syslogd"
	"log/syslog"
	"net"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"
	"time"
)

var logger *syslog.Writer

type RemoteHeartbeatMessage struct {
	Type       string
	SyslogTags []string
}

type ClientConfigMessage struct {
	SyslogTags     []string
	NetworkAddress net.UDPAddr
	LastHeartbeat  time.Time
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
		clientConfigMessage.SyslogTags = make([]string, len(remoteHeartbeatMessage.SyslogTags))
		copy(clientConfigMessage.SyslogTags, remoteHeartbeatMessage.SyslogTags)
		clients <- clientConfigMessage
	}
}

func serveSyslogs(conn *net.UDPConn, clients chan ClientConfigMessage, signals chan os.Signal) {
	var err error

	deadClientsTicker := time.NewTicker(time.Second * 10).C
	activeClients := make(map[string]map[string]ClientConfigMessage)
	// logs are about 330 bytes, lets cache up to a 50MBs worth of logs
	syslogChannel := make(chan syslogparser.LogParts, 150000)

	syslogServer := syslogd.NewServer()
	err = syslogServer.ListenUDP(":5515")
	checkErrorFatal(err)
	syslogServer.Start(syslogChannel)

	for {
		select {
		case clientConfigMessage := <-clients:
			addClient(activeClients, clientConfigMessage)
		case <-deadClientsTicker:
			pruneDeadClients(activeClients)
		case logParts := <-syslogChannel:
			sendLogsToClients(conn, activeClients, logParts)
		case signal := <-signals:
			fmt.Println("\nExiting caught signal:", signal)
			return
		}
	}
}

func sendLogsToClients(conn *net.UDPConn, activeClients map[string]map[string]ClientConfigMessage, logParts syslogparser.LogParts) {
	// Example rsyslog message: 'Apr  3 19:23:40 apply02 nginx-apply: this is the message'
	unformattedTime := logParts["timestamp"].(time.Time)
	formattedTime := unformattedTime.Format(time.Stamp)
	logMessage := fmt.Sprintf("%s %s %s%s %s\n", formattedTime, logParts["hostname"], logParts["tag"], ":", logParts["content"])
	syslogTag := fmt.Sprintf("%s", logParts["tag"])

	if clients, ok := activeClients[syslogTag]; ok {
		for _, clientConfigMessage := range clients {
			conn.WriteToUDP([]byte(logMessage), &clientConfigMessage.NetworkAddress)
		}
	}

	// Send all messages to "*" clients
	if clients, ok := activeClients["*"]; ok {
		for _, clientConfigMessage := range clients {
			conn.WriteToUDP([]byte(logMessage), &clientConfigMessage.NetworkAddress)
		}
	}
}

func addClient(activeClients map[string]map[string]ClientConfigMessage, clientConfigMessage ClientConfigMessage) {
	var newClientMap map[string]ClientConfigMessage
	for _, syslogTag := range clientConfigMessage.SyslogTags {
		if clients, ok := activeClients[syslogTag]; ok {
			clients[clientConfigMessage.NetworkAddress.String()] = clientConfigMessage
		} else {
			newClientMap = make(map[string]ClientConfigMessage)
			newClientMap[clientConfigMessage.NetworkAddress.String()] = clientConfigMessage
			activeClients[syslogTag] = newClientMap
		}
	}
}

func pruneDeadClients(activeClients map[string]map[string]ClientConfigMessage) {
	for syslogTag, clients := range activeClients {
		for clientNetworkAddress, clientConfigMessage := range clients {
			lastHeartbeatDuration := time.Now().Sub(clientConfigMessage.LastHeartbeat)
			if lastHeartbeatDuration.Seconds() >= 10 {
				delete(clients, clientNetworkAddress)
			}
		}

		if len(clients) == 0 {
			delete(activeClients, syslogTag)
		} else {
			logger.Info(fmt.Sprintf("Tag: %s, Clients: %d\n", syslogTag, len(clients)))
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

func init() {
	var err error
	logger, err = syslog.Dial("udp", "localhost:514", syslog.LOG_INFO|syslog.LOG_DAEMON, "logsprinkler")
	checkErrorFatal(err)
}

func main() {
	var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
	var memprofile = flag.String("memprofile", "", "write memory profile to this file")
	flag.Parse()

	logger.Info("Starting up")

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		checkErrorFatal(err)
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	UDPAddr := net.UDPAddr{Port: 5514}
	conn, err := net.ListenUDP("udp", &UDPAddr)
	checkErrorFatal(err)
	clients := make(chan ClientConfigMessage)

	go handleClientHeartbeats(conn, clients)
	serveSyslogs(conn, clients, signals)

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		checkErrorFatal(err)
		pprof.Lookup("heap").WriteTo(f, 0)
		f.Close()
	}
}
