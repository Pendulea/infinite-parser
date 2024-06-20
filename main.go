package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"pendulev2/pairs"
	setlib "pendulev2/set2"
	engine "pendulev2/task-engine"

	// engine "pendulev2/task-engine"

	// engine "pendule/task-engine"
	"pendulev2/util"
	"strings"
	"sync"
	"time"

	pcommon "github.com/pendulea/pendule-common"

	// "pendule/rpc"
	"syscall"

	"github.com/gorilla/websocket"
	// "github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

var ALLOWED_STABLE_SYMBOL_LIST = []string{"USDT", "USDC"}

var activeSets = make(setlib.WorkingSets)
var server *http.Server
var wsConns sync.Map

func main() {
	initLogger()
	pcommon.Env.Init()
	log.Info("MIN_TIME_FRAME: ", pcommon.Env.MIN_TIME_FRAME)
	engine.Engine.Init(&activeSets)

	if os.Getenv("CSV_DIR") == "" {
		log.Fatal("CSV_DIR is not set")
	}

	pairs.Init(&activeSets, os.Getenv("PAIRS_PATH"))
	// rpc.Init(&activeSets, pairs.Init(&activeSets, os.Getenv("PAIRS_PATH")))
	// go initWS()
	go initScheduleAutoCSVDelete()

	go func() {
		time.Sleep(time.Second * 2)
		for _, set := range activeSets {
			set.AddTimeframe(time.Hour*4, engine.Engine.AddTimeframeIndexing)
		}
	}()

	sigs := make(chan os.Signal, 1)
	// Create a channel to communicate that the signal has been handled
	done := make(chan bool, 1)

	// Register the channel to receive notifications for specific signals
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	// Start a goroutine that will handle the signals
	go func() {
		<-sigs // Block until a signal is received
		cleanup()
		done <- true // Signal that handling is complete
	}()

	<-done // Block until the signal has been handled
	log.Info("Exiting...")
}

func cleanup() {
	log.Info("Shutting down server and WebSocket connections...")

	if server != nil {
		// Create a context with a timeout to ensure the server shuts down gracefully
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := server.Shutdown(ctx); err != nil {
			log.Errorf("Server shutdown failed: %s", err)
		} else {
			log.Info("Server gracefully stopped")
		}
	}

	// Close all active WebSocket connections
	wsConns.Range(func(key, value interface{}) bool {
		conn := value.(*websocket.Conn)
		conn.Close()
		return true
	})

	engine.Engine.Quit()
	activeSets.StopAll()
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	// Allow connections from any Origin
	CheckOrigin: func(r *http.Request) bool { return true },
}

// func handler(w http.ResponseWriter, r *http.Request) {
// 	conn, err := upgrader.Upgrade(w, r, nil)
// 	if err != nil {
// 		log.Println(err)
// 		return
// 	}
// 	defer conn.Close()
// 	wsConns.Store(conn.RemoteAddr(), conn)

// 	defer func() {
// 		wsConns.Delete(conn.RemoteAddr())
// 		conn.Close()
// 	}()

// 	for {
// 		messageType, message, err := conn.ReadMessage()
// 		if err != nil {
// 			log.Println("read:", err)
// 			break
// 		}
// 		response := pcommon.RPC.HandleServerRequest(message, rpc.Service)
// 		jsonResponse, err := json.Marshal(response)
// 		if err != nil {
// 			log.Println("json:", err)
// 			break
// 		}
// 		err = conn.WriteMessage(messageType, jsonResponse)
// 		if err != nil {
// 			log.Println("write:", err)
// 			break
// 		}
// 	}
// }

func initScheduleAutoCSVDelete() {
	csvDIR := os.Getenv("CSV_DIR")

	task := func() {
		list, err := pcommon.File.GetSortedFilenamesByDate(csvDIR)
		if err != nil {
			log.WithFields(log.Fields{
				"err": err.Error(),
			}).Error("Error getting sorted filenames by date from schedule auto CSV delete")
		}
		now := time.Now().UTC().Unix()
		for _, f := range list {
			if now-86_400 > f.Time {
				err := os.Remove(filepath.Join(csvDIR, f.Name))
				if err != nil {
					log.WithFields(log.Fields{
						"err": err.Error(),
					}).Error("Error removing file from schedule auto CSV delete")
				}
				log.WithFields(log.Fields{
					"file": f.Name,
				}).Info("File CSV removed")
			}
		}
	}

	task()
	util.ScheduleTask(context.Background(), 3, 3, task)
}

// func initWS() {
// 	mux := http.NewServeMux()
// 	mux.HandleFunc("/", handler)
// 	mux.HandleFunc("/download/", fileDownloadHandler)

// 	server = &http.Server{
// 		Addr:    ":" + pcommon.Env.PARSER_SERVER_PORT,
// 		Handler: mux,
// 	}

// 	log.Infof("Websocket server running on wss://localhost:%s", pcommon.Env.PARSER_SERVER_PORT)
// 	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
// 		log.Fatalf("ListenAndServe(): %s", err)
// 	}
// }

func initLogger() {
	// Log as JSON instead of the default ASCII formatter.
	log.SetFormatter(&log.TextFormatter{
		ForceColors:      true,                  // Force colored log output even if stdout is not a tty.
		FullTimestamp:    true,                  // Enable logging the full timestamp instead of just the time passed since application started.
		TimestampFormat:  "2006-01-02 15:04:05", // Set the format for the timestamp.
		DisableTimestamp: false,                 // Do not disable printing timestamps.
	})

	// Output to stdout instead of the default stderr, could also be a file.
	log.SetOutput(os.Stdout)

	// Only log the warning severity or above.
	log.SetLevel(log.InfoLevel)
}

func fileDownloadHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	// Allow all origins
	w.Header().Set("Access-Control-Allow-Origin", "*")
	// Allow specific methods
	w.Header().Set("Access-Control-Allow-Methods", "GET")
	// Allow specific headers
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	fileName := strings.TrimPrefix(r.URL.Path, "/download/")
	if fileName == "" {
		http.Error(w, "File name is missing", http.StatusBadRequest)
		return
	}

	fileName = filepath.Clean(fileName)
	filePath := filepath.Join(os.Getenv("CSV_DIR"), fileName)
	file, err := os.Open(filePath)
	if err != nil {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}
	defer file.Close()

	// Get the file's content type
	fileStat, err := file.Stat()
	if err != nil {
		http.Error(w, "Could not get file info", http.StatusInternalServerError)
		return
	}

	fileSize := fileStat.Size()
	name := fileStat.Name()

	// Set the headers
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", name))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", fileSize))

	buffer := make([]byte, 1024*64) // 64KB buffer
	totalBytesSent := int64(0)
	ticker := time.NewTicker(4 * time.Second)
	done := make(chan struct{})

	go func() {
		for {
			select {
			case <-ticker.C:
				percentage := float64(totalBytesSent) / float64(fileSize) * 100
				log.WithFields(log.Fields{
					"file":     name,
					"progress": percentage,
				}).Info("File download progress")
			case <-done:
				return
			}
		}
	}()

	for {
		n, err := file.Read(buffer)
		if err != nil {
			if err.Error() != "EOF" {
				http.Error(w, "Error reading file", http.StatusInternalServerError)
			}
			break
		}

		if n == 0 {
			break
		}

		_, err = w.Write(buffer[:n])
		if err != nil {
			http.Error(w, "Error writing response", http.StatusInternalServerError)
			break
		}

		totalBytesSent += int64(n)
	}
	ticker.Stop()
	close(done)
	log.WithFields(log.Fields{
		"file": name,
		"in":   "+" + pcommon.Format.AccurateHumanize(time.Since(start)),
	}).Info("File download completed")
}
