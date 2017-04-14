// vim: noexpandtab tabstop=2 shiftwidth=2:
// syslog sprinkler daemon
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/jeromer/syslogparser"
	"github.com/lollipopman/syslogd"
	"net"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"
	"strconv"
	"time"
)

func init() {
	// Output to stdout instead of the default stderr, could also be a file.
	log.SetOutput(os.Stderr)

	// Only log the warning severity or above.
	log.SetLevel(log.DebugLevel)
}

func isEAGAIN(err error) bool {
	if pe, ok := err.(*os.PathError); ok {
		if errno, ok := pe.Err.(syscall.Errno); ok && (errno == syscall.EAGAIN || errno == syscall.EPIPE) {
			return true
		}
	}
	return false
}

type TagConfig struct {
	Clients     int
	NewClients  chan int
	DeadClients chan int
	Logs        chan syslogparser.LogParts
	Done        chan int
}

type PipeStatus struct {
	Name     string
	File     *os.File
	Open     bool
}

type RemoteHeartbeatMessage struct {
	Type       string
	Pid        int
	SyslogTags []string
}

type ClientConfigMessage struct {
	SyslogTags    []string
	Pid           int
	LastHeartbeat time.Time
}

func handleClientHeartbeats(clientHeartbeats chan ClientConfigMessage) {
	var remoteHeartbeatMessage RemoteHeartbeatMessage
	var clientConfigMessage ClientConfigMessage
	byteHeartbeatMessage := make([]byte, 1024)

	UDPAddr := net.UDPAddr{Port: 5514}
	conn, err := net.ListenUDP("udp", &UDPAddr)
	checkErrorFatal(err)

	for {
		bytesRead, _, err := conn.ReadFromUDP(byteHeartbeatMessage)
		if checkError(err) {
			continue
		}
		err = json.Unmarshal(byteHeartbeatMessage[:bytesRead], &remoteHeartbeatMessage)
		if checkError(err) {
			continue
		}
		clientConfigMessage.Pid = remoteHeartbeatMessage.Pid
		clientConfigMessage.LastHeartbeat = time.Now()
		clientConfigMessage.SyslogTags = make([]string, len(remoteHeartbeatMessage.SyslogTags))
		copy(clientConfigMessage.SyslogTags, remoteHeartbeatMessage.SyslogTags)
		clientHeartbeats <- clientConfigMessage
	}
}

func monitorClients(clientHeartbeats chan ClientConfigMessage, newClients chan ClientConfigMessage, deadClients chan ClientConfigMessage) {
	clients := make(map[int]ClientConfigMessage)
	deadClientsTicker := time.NewTicker(time.Second * 10).C

	for {
		select {
		case clientConfigMessage := <-clientHeartbeats:
			if _, ok := clients[clientConfigMessage.Pid]; !ok {
				log.Info("new client")
				newClients <-clientConfigMessage
			}
			clients[clientConfigMessage.Pid] = clientConfigMessage
		case <-deadClientsTicker:
			for pid, clientConfigMessage := range clients {
				lastHeartbeatDuration := time.Now().Sub(clientConfigMessage.LastHeartbeat)
				if lastHeartbeatDuration.Seconds() >= 10 {
					delete(clients, pid)
					deadClients <- clientConfigMessage
				}
			}
		}
	}
}

func serveSyslogs(newClients chan ClientConfigMessage, deadClients chan ClientConfigMessage, signals chan os.Signal) {
	var err error

	activeTags := make(map[string]*TagConfig)

	// logs are about 330 bytes, lets cache up to a 50MBs worth of logs
	syslogChannel := make(chan syslogparser.LogParts, 150000)

	syslogServer := syslogd.NewServer()
	err = syslogServer.ListenUDP(":5515")
	checkErrorFatal(err)
	syslogServer.Start(syslogChannel)

	for {
		select {
		case clientConfigMessage := <-newClients:
			log.Info("serveSyslogs: new client")
			addClient(activeTags, clientConfigMessage)
			log.Info("serveSyslogs: Client added")
			log.Info(activeTags)
		case clientConfigMessage := <-deadClients:
			pruneDeadClients(activeTags, clientConfigMessage)
		case logParts := <-syslogChannel:
			log.Info("serveSyslogs: logs received, tag: " + logParts["tag"].(string))
			if tagConfig, ok := activeTags[logParts["tag"].(string)]; ok {
				tagConfig.Logs <- logParts
			}
		case signal := <-signals:
			fmt.Println("\nExiting caught signal:", signal)
			return
		}
	}
}

func tagSender(tag string, newClients chan int, deadClients chan int, logs chan syslogparser.LogParts, done chan int) {
	var err error
	var n int
	clients := make(map[int]*PipeStatus)
	log.Info("tagSender: startup!")
	tagsPath := "./tags/" + tag
	if _, err := os.Stat(tagsPath); os.IsNotExist(err) {
		os.Mkdir(tagsPath, 0770)
	}
	for {
		select {
		case newClient := <-newClients:
			log.Info("tagSender: new client!")
			fileName := "./tags/" + tag + "/" + strconv.Itoa(newClient)
			syscall.Mkfifo(fileName, 0666)
			file, err := os.OpenFile(fileName, os.O_WRONLY|syscall.O_NONBLOCK, os.ModeNamedPipe)
			if checkError(err) {
				clients[newClient] = &PipeStatus{
					Name: fileName,
					File: nil,
					Open: false,
				}
			} else {
				clients[newClient] = &PipeStatus{
					Name: fileName,
					File: file,
					Open: true,
				}
			}
	  case deadClient := <-deadClients:
			if pipeStatus, ok := clients[deadClient]; ok {
				removePipe(pipeStatus)
				delete(clients, deadClient)
			}
		case logParts := <-logs:
			log.Info("tagSender: logs!, len: " + strconv.Itoa(len(clients)))
			unformattedTime := logParts["timestamp"].(time.Time)
			formattedTime := unformattedTime.Format(time.Stamp)
			logMessage := fmt.Sprintf("%s %s %s%s %s\n", formattedTime, logParts["hostname"], logParts["tag"], ":", logParts["content"])
			for client, pipeStatus := range clients {
				log.Info("tagSender: writing log for, " + strconv.Itoa(client))
				if ! pipeStatus.Open {
					file, err := os.OpenFile(pipeStatus.Name, os.O_WRONLY|syscall.O_NONBLOCK, os.ModeNamedPipe)
					if ! checkError(err) {
						clients[client].File = file
						clients[client].Open = true
					}
				}
				if pipeStatus.Open {
					n, err = pipeStatus.File.WriteString(logMessage)
					log.Info("tagSender: bytes written: " + strconv.Itoa(n))
					if ! isEAGAIN(err) {
						checkErrorFatal(err)
					}
				}
			}
		case <-done:
			log.Info("tagSender: done, len: " + strconv.Itoa(len(clients)))
			for _, pipeStatus := range clients {
				removePipe(pipeStatus)
			}
			return
		}
	}
}

func removePipe(pipeStatus *PipeStatus) {
	var err error
	if pipeStatus.Open {
		err = pipeStatus.File.Close()
		if ! checkError(err) {
			err = os.Remove(pipeStatus.File.Name())
			checkError(err)
		}
	}
}

func addClient(activeTags map[string]*TagConfig, clientConfigMessage ClientConfigMessage) {
  log.Info("addClient")
	for _, syslogTag := range clientConfigMessage.SyslogTags {
		if tagConfig, ok := activeTags[syslogTag]; ok {
			log.Info("addClient: existing")
			tagConfig.NewClients <- clientConfigMessage.Pid
			tagConfig.Clients++
		} else {
			log.Info("addClient: new")
			tagConfig := &TagConfig{
				Clients: 0,
				NewClients:  make(chan int),
				DeadClients: make(chan int),
				Logs:        make(chan syslogparser.LogParts),
				Done:        make(chan int),
			}
			log.Info("addClient: go")
			go tagSender(syslogTag, tagConfig.NewClients, tagConfig.DeadClients, tagConfig.Logs, tagConfig.Done)
			log.Info("addClient: go done")
			tagConfig.NewClients <- clientConfigMessage.Pid
			log.Info("addClient: tagSender received msg")
			tagConfig.Clients++
			activeTags[syslogTag] = tagConfig
			log.Info("addClient: done adding new")
			log.Info(tagConfig)
		}
	}
}

func pruneDeadClients(activeTags map[string]*TagConfig, clientConfigMessage ClientConfigMessage) {
	for _, syslogTag := range clientConfigMessage.SyslogTags {
		if tagConfig, ok := activeTags[syslogTag]; ok {
			tagConfig.Clients--
			if tagConfig.Clients == 0 {
				tagConfig.Done <- 1
			  delete(activeTags, syslogTag)
			} else {
				tagConfig.DeadClients <- clientConfigMessage.Pid
			}
		}
	}
}

func checkErrorFatal(err error) {
	if err != nil {
		log.Fatal(fmt.Sprintf("Error %T, %s", err, err.Error()))
		os.Exit(1)
	}
}

func checkError(err error) bool {
	foundError := false
	if err != nil {
		foundError = true
		log.Warning(fmt.Sprintf("Error %T, %s", err, err.Error()))
	}
	return foundError
}

func main() {
	var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
	var memprofile = flag.String("memprofile", "", "write memory profile to this file")
	flag.Parse()

	log.Info("Starting up")

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		checkErrorFatal(err)
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	tagsPath := "./tags"
	if _, err := os.Stat(tagsPath); os.IsNotExist(err) {
		os.Mkdir(tagsPath, 0770)
	}

	clientHeartbeats := make(chan ClientConfigMessage)
	newClients := make(chan ClientConfigMessage)
	deadClients := make(chan ClientConfigMessage)

	go handleClientHeartbeats(clientHeartbeats)
	go monitorClients(clientHeartbeats, newClients, deadClients)
	log.Info("Serving syslogs")
	serveSyslogs(newClients, deadClients, signals)

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		checkErrorFatal(err)
		pprof.Lookup("heap").WriteTo(f, 0)
		f.Close()
	}
}
