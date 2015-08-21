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
	"sort"
	"strings"
	"syscall"
	"time"
)

var logger *syslog.Writer

type ClientGroup struct {
	GroupSyslogTags []string
	Clients         map[string]ClientConfigMessage
}

type RemoteHeartbeatMessage struct {
	Type       string
	SyslogTags []string
}

type ClientConfigMessage struct {
	SyslogTags          []string
	SyslogTagIdentifier string
	NetworkAddress      net.UDPAddr
	LastHeartbeat       time.Time
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
		clientConfigMessage.SyslogTags = make([]string, len(remoteHeartbeatMessage.SyslogTags))
		copy(clientConfigMessage.SyslogTags, remoteHeartbeatMessage.SyslogTags)
		sort.Strings(clientConfigMessage.SyslogTags)
		clientConfigMessage.SyslogTagIdentifier = strings.Join(clientConfigMessage.SyslogTags, ",")
		clients <- clientConfigMessage
	}
}

func serveSyslogs(conn *net.UDPConn, clients chan ClientConfigMessage, signals chan os.Signal) {
	var searchIndex int
	var err error
	var signal os.Signal
	var newClientGroup ClientGroup
	var newClientMap map[string]ClientConfigMessage

	pruneDeadClientsTicker := time.NewTicker(time.Second * 10).C
	activeClients := make(map[string]ClientGroup)
	syslogChannel := make(chan syslogparser.LogParts, 1)

	syslogServer := syslogd.NewServer()
	err = syslogServer.ListenUDP(":5515")
	checkErrorFatal(err)
	syslogServer.Start(syslogChannel)

	for {
		select {
		case clientConfigMessage := <-clients:
			if clientGroup, ok := activeClients[clientConfigMessage.SyslogTagIdentifier]; ok {
				clientGroup.Clients[clientConfigMessage.NetworkAddress.String()] = clientConfigMessage
			} else {
				newClientGroup.GroupSyslogTags = make([]string, len(clientConfigMessage.SyslogTags))
				copy(newClientGroup.GroupSyslogTags, clientConfigMessage.SyslogTags)
				newClientMap = make(map[string]ClientConfigMessage)
				newClientGroup.Clients = newClientMap
				newClientGroup.Clients[clientConfigMessage.NetworkAddress.String()] = clientConfigMessage
				activeClients[clientConfigMessage.SyslogTagIdentifier] = newClientGroup
			}
		case <-pruneDeadClientsTicker:
			pruneDeadClients(activeClients)
		case logParts := <-syslogChannel:
			logTag := fmt.Sprintf("%s", logParts["tag"])
			for clientGroupIdentifier, clientGroup := range activeClients {
				searchIndex = sort.SearchStrings(clientGroup.GroupSyslogTags, logTag)
				if clientGroupIdentifier == "*" || (searchIndex < len(clientGroup.GroupSyslogTags) && clientGroup.GroupSyslogTags[searchIndex] == logTag) {
					// Example rsyslog message: 'Apr  3 19:23:40 apply02 nginx-apply: this is the message'
					unformattedTime := logParts["timestamp"].(time.Time)
					formattedTime := unformattedTime.Format(time.Stamp)
					logMessage := fmt.Sprintf("%s %s %s%s %s\n", formattedTime, logParts["hostname"], logParts["tag"], ":", logParts["content"])
					for _, clientConfigMessage := range clientGroup.Clients {
						conn.WriteToUDP([]byte(logMessage), &clientConfigMessage.NetworkAddress)
					}
				}
			}
		case signal = <-signals:
			fmt.Println("\nExiting caught signal:", signal)
			return
		}
	}
}

func pruneDeadClients(activeClients map[string]ClientGroup) {
	var totalNumberOfClients int
	var numberOfClients int
	numberOfClients = 0
	for clientGroupIdentifier, clientGroup := range activeClients {
		for clientNetworkAddress, clientConfigMessage := range clientGroup.Clients {
			lastHeartbeatDuration := time.Now().Sub(clientConfigMessage.LastHeartbeat)
			if lastHeartbeatDuration.Seconds() >= 10 {
				delete(clientGroup.Clients, clientNetworkAddress)
			}
		}

		if len(clientGroup.Clients) == 0 {
			delete(activeClients, clientGroupIdentifier)
		} else {
			numberOfClients = len(clientGroup.Clients)
			totalNumberOfClients += numberOfClients
			fmt.Printf("Group [%s], Clients %d\n", clientGroupIdentifier, numberOfClients)
		}
	}
	fmt.Println("Total Clients:", totalNumberOfClients)
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
	var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
	var memprofile = flag.String("memprofile", "", "write memory profile to this file")
	flag.Parse()

	logger.Info("logsprinkler starting up")

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
