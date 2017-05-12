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
	"strconv"
	"syscall"
	"time"
)

func init() {
	// Output to stdout instead of the default stderr, could also be a file.
	log.SetOutput(os.Stderr)
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
	Name string
	File *os.File
	Open bool
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

	UDPAddr, err := net.ResolveUDPAddr("udp", "localhost:5514")
	checkErrorFatal(err)
	conn, err := net.ListenUDP("udp", UDPAddr)
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
				log.Infof("New client heartbeat, PID: %v", clientConfigMessage.Pid)
				newClients <- clientConfigMessage
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
	err = syslogServer.ListenUDP("localhost:5515")
	checkErrorFatal(err)
	syslogServer.Start(syslogChannel)

	logCounter := 0

	for {
		select {
		case clientConfigMessage := <-newClients:
			addClient(activeTags, clientConfigMessage)
		case clientConfigMessage := <-deadClients:
			pruneDeadClients(activeTags, clientConfigMessage)
		case logParts := <-syslogChannel:
			logCounter++
			if tagConfig, ok := activeTags[logParts["tag"].(string)]; ok {
				tagConfig.Logs <- logParts
			}
		case signal := <-signals:
			log.Infof("Exiting caught signal: %v", signal)
			log.Infof("Served %v logs:", logCounter)
			return
		}
	}
}

func tagSender(tag string, newClients chan int, deadClients chan int, logs chan syslogparser.LogParts, done chan int) {
	var err error
	var n int
	clients := make(map[int]*PipeStatus)
	log.Infof("tagSender for tag, %v, starting", tag)
	tagsPath := "./tags/" + tag
	if _, err := os.Stat(tagsPath); os.IsNotExist(err) {
		os.Mkdir(tagsPath, 0770)
	}
	for {
		select {
		case newClient := <-newClients:
			fileName := "./tags/" + tag + "/" + strconv.Itoa(newClient)
			log.Debugf("tagSender[%v]: new client!", fileName)
			err = syscall.Mkfifo(fileName, 0666)
			if err != nil {
				log.Fatalf("Unable to create fifo: %v", fileName)
			}
			clients[newClient] = &PipeStatus{
				Name: fileName,
				File: nil,
				Open: false,
			}
		case deadClient := <-deadClients:
			if pipeStatus, ok := clients[deadClient]; ok {
				removePipe(deadClient, pipeStatus)
				delete(clients, deadClient)
			}
		case logParts := <-logs:
			unformattedTime := logParts["timestamp"].(time.Time)
			formattedTime := unformattedTime.Format(time.Stamp)
			logMessage := fmt.Sprintf("%s %s %s%s %s\n", formattedTime, logParts["hostname"], logParts["tag"], ":", logParts["content"])
			for client, pipeStatus := range clients {
				if !pipeStatus.Open {
					file, err := os.OpenFile(pipeStatus.Name, os.O_WRONLY|syscall.O_NONBLOCK, os.ModeNamedPipe)
					if !checkError(err) {
						clients[client].File = file
						clients[client].Open = true
					}
				}
				if pipeStatus.Open {
					n, err = pipeStatus.File.WriteString(logMessage)
					if n-len(logMessage) > 0 {
						log.Debugf("tagSender: %v bytes not written", n-len(logMessage))
					}
					if !isEAGAIN(err) {
						checkErrorFatal(err)
					}
				}
			}
		case <-done:
			log.Infof("tagSender %v closing", tag)
			for deadClient, pipeStatus := range clients {
				removePipe(deadClient, pipeStatus)
			}
			return
		}
	}
}

func removePipe(deadClient int, pipeStatus *PipeStatus) {
	var err error
	log.Infof("Removing dead client %v's pipe, %v", deadClient, pipeStatus.Name)
	if pipeStatus.Open {
		err = pipeStatus.File.Close()
		if !checkError(err) {
			err = os.Remove(pipeStatus.File.Name())
			checkError(err)
		}
	}
}

func addClient(activeTags map[string]*TagConfig, clientConfigMessage ClientConfigMessage) {
	for _, syslogTag := range clientConfigMessage.SyslogTags {
		if tagConfig, ok := activeTags[syslogTag]; ok {
			tagConfig.NewClients <- clientConfigMessage.Pid
			tagConfig.Clients++
		} else {
			tagConfig := &TagConfig{
				Clients:     0,
				NewClients:  make(chan int),
				DeadClients: make(chan int),
				Logs:        make(chan syslogparser.LogParts),
				Done:        make(chan int),
			}
			go tagSender(syslogTag, tagConfig.NewClients, tagConfig.DeadClients, tagConfig.Logs, tagConfig.Done)
			tagConfig.NewClients <- clientConfigMessage.Pid
			tagConfig.Clients++
			activeTags[syslogTag] = tagConfig
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
		log.Fatalf("Error %T, %s", err, err.Error())
		os.Exit(1)
	}
}

func checkError(err error) bool {
	foundError := false
	if err != nil {
		foundError = true
		log.Warningf("Error %T, %s", err, err.Error())
	}
	return foundError
}

func main() {
	var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
	var memprofile = flag.String("memprofile", "", "write memory profile to this file")
	var debug = flag.Bool("d", false, "Enable debug mode")
	flag.Parse()

	if *debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

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
	serveSyslogs(newClients, deadClients, signals)

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		checkErrorFatal(err)
		pprof.Lookup("heap").WriteTo(f, 0)
		f.Close()
	}
}
