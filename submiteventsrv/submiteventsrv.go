package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/net/context"

	"github.com/vsysoev/goeventstore/evstore"
	"github.com/vsysoev/goeventstore/property"
	"github.com/vsysoev/goeventstore/wsock"
)

type (
	Connector wsock.Connector
)

// DONE:60 Events might be submitted through websocket
func handleClientRequest(ctx context.Context, c Connector, e *evstore.Connection) {
	var ev map[string]interface{}
	fromWS, toWS, doneCh := c.GetChannels()
Loop:
	for {
		select {
		case <-ctx.Done():
		case <-doneCh:
			break Loop
		case msg := <-fromWS:
			//DONE:0 Need message format check and report in case of failure
			response := wsock.MessageT{"reply": "ok"}
			seqid := ""
			if val, ok := (*msg)["sequenceid"].(string); ok {
				seqid = val
			}
			tag := ""
			if val, ok := (*msg)["tag"].(string); ok {
				tag = val
			} else {
				response["reply"] = "ERROR"
				response["msg"] = "No tag"
			}
			if val, ok := (*msg)["event"].(map[string]interface{}); ok {
				ev = val
			} else {
				response["reply"] = "ERROR"
				response["msg"] = "No event"
			}
			if response["reply"] == "ok" {
				err := e.Committer().SubmitMapStringEvent(seqid, tag, ev)
				if err != nil {
					response["reply"] = "ERROR"
					response["msg"] = "Submit to eventstore failed"
				}
			}
			toWS <- &response
			break
		case <-time.After(time.Millisecond * 10):
			break
		}
	}
}

// DOING:0 Events might be submitted through REST interface
func processClientConnection(s *wsock.Server, props property.PropSet) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log.Println("Enter processClientConnection")
	addCh, delCh, doneCh, _ := s.GetChannels()
	log.Println("Get server channels", addCh, delCh, doneCh)
Loop:
	for {
		select {
		case <-doneCh:
			log.Println("doneCh got message")
			break Loop
		case <-ctx.Done():
			log.Println("Context destroyed")
			break Loop
		case cli := <-addCh:
			log.Println("processClientConnection got add client notification", cli.Request().FormValue("id"))
			streamName := cli.Conn().Request().FormValue("stream")
			if streamName == "" {
				streamName = props["mongodb.stream"]
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			log.Println("Stream name", streamName)
			evStore, err := evstore.Dial(props["mongodb.url"], props["mongodb.db"], streamName)
			defer evStore.Close()
			if err != nil {
				log.Fatalln("Error connecting to event store. ", err)
				return
			}
			go handleClientRequest(ctx, cli, evStore)
			break
		case cli := <-delCh:
			log.Println("delCh go client", cli)
			break
		}
	}
	log.Println("processClientConnection exited")
}
func handlePostRequest(w http.ResponseWriter, req *http.Request) {
	props := property.Init()
	data := make([]byte, 2048)
	log.Println(req.URL, req.Method)
	n, err := req.Body.Read(data)
	if n == 0 {
		io.WriteString(w, "No data posted")
		return
	}
	if err != nil && err != io.EOF {
		io.WriteString(w, "Error while reading request body")
		return
	}
	streamName := props["mongodb.stream"]
	if req.FormValue("stream") != "" {
		streamName = req.FormValue("stream")
	}
	m := make(map[string]interface{})
	err = json.Unmarshal(data[:n], &m)
	if err != nil {
		log.Println("Error parsing data", err)
		log.Println(string(data))
		io.WriteString(w, "Error parsing request data")
		return
	}
	seqid := ""
	if val, ok := m["sequenceid"].(string); ok {
		seqid = val
	}
	tag := ""
	if val, ok := m["tag"].(string); ok {
		tag = val
	} else {
		log.Println("Error no tag in event", err)
		io.WriteString(w, "Error no tag in event")
		return
	}
	ev, err := evstore.Dial(props["mongodb.url"], props["mongodb.db"], streamName)
	if err != nil {
		log.Println("Error connecting to event store", err)
		io.WriteString(w, "Error connecting to event store")
		return
	}
	defer ev.Close()
	err = ev.Committer().SubmitMapStringEvent(seqid, tag, m["event"].(map[string]interface{}))
	if err != nil {
		log.Println("Error submitting event to event store", err)
		io.WriteString(w, "Error submitting event to event store")
		return
	}
	io.WriteString(w, "{\"reply\":\"ok\"}")

}
func main() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		select {
		case <-c:
			syscall.Exit(0)
		}
	}()
	props := property.Init()
	//DONE:10 evstore should be connected when user connected. Because in request should be defined stream to submit events.
	wsServer := wsock.NewServer(props["submitevents.uri"])
	if wsServer == nil {
		log.Fatalln("Error creating new websocket server")
	}
	go processClientConnection(wsServer, props)
	go wsServer.Listen()
	http.HandleFunc(props["postevents.uri"], handlePostRequest)
	err := http.ListenAndServe(props["submitevents.url"], nil)
	if err != nil {
		log.Fatalln("Error while ListenAndServer", err)
	}
}
