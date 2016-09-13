package main

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"context"

	"github.com/vsysoev/goeventstore/evstore"
	"github.com/vsysoev/goeventstore/property"
	"github.com/vsysoev/goeventstore/wsock"
)

//TODO:0 Implement ONLY HTTPS post of events
//DOING:0 Implements baseauth authorization

func handleClientRequest(ctx context.Context, c *wsock.ClientInterface, e evstore.Connection) {
	var ev map[string]interface{}
	fromWS, toWS, doneCh := (*c).GetChannels()
Loop:
	for {
		select {
		case <-ctx.Done():
		case <-doneCh:
			break Loop
		case msg := <-fromWS:
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
				err := e.Committer(tag).SubmitMapStringEvent(seqid, tag, ev)
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

func processClientConnection(s wsock.ServerInterface, props property.PropSet) {
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
			log.Println("processClientConnection got add client notification", (*cli).Request().FormValue("id"))
			streamName := (*cli).Conn().Request().FormValue("stream")
			if streamName == "" {
				streamName = props["mongodb.stream"]
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			log.Println("Stream name", streamName)
			evStore, err := evstore.Dial(props["mongodb.url"], props["mongodb.db"])
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
func readRequestBody(req *http.Request) ([]byte, error) {
	data := make([]byte, 2048)
	n, err := req.Body.Read(data)
	if n == 0 {
		return nil, errors.New("No data posted")
	}
	if err != nil && err != io.EOF {
		return nil, err
	}
	return data[:n], err
}
func parseData(data []byte) (map[string]interface{}, error) {
	m := make(map[string]interface{})
	err := json.Unmarshal(data, &m)
	if err != nil {
		return nil, errors.New("Error parsing request data")
	}
	return m, nil
}
func checkAuth() error {
	return errors.New("Not Implemented")
}
func extractEvent(m map[string]interface{}) (string, string, map[string]interface{}, error) {
	seqid := ""
	if val, ok := m["sequenceid"].(string); ok {
		seqid = val
	}
	tag := ""
	if val, ok := m["tag"].(string); ok {
		tag = val
	} else {
		return "", "", nil, errors.New("Error no tag in event")
	}
	event := map[string]interface{}{}
	if val, ok := m["event"].(map[string]interface{}); ok {
		event = val
	} else {
		return "", "", nil, errors.New("Error no event in message")
	}
	return seqid, tag, event, nil
}

func submitEvent2Datastore(url string,
	database string,
	streamName string,
	seqid string,
	tag string,
	event map[string]interface{}) error {
	ev, err := evstore.Dial(url, database)
	if err != nil {
		return err
	}
	defer ev.Close()
	err = ev.Committer(tag).SubmitMapStringEvent(seqid, tag, event)
	if err != nil {
		return err
	}
	return nil
}
func handlePostRequest(w http.ResponseWriter, req *http.Request) {
	props := property.Init()
	data, err := readRequestBody(req)
	if err != nil {
		io.WriteString(w, err.Error())
		return
	}
	msg, err := parseData(data)
	if err != nil {
		io.WriteString(w, err.Error())
		return
	}
	streamName := props["mongodb.stream"]
	if req.FormValue("stream") != "" {
		streamName = req.FormValue("stream")
	}
	seqid, tag, event, err := extractEvent(msg)
	if err != nil {
		io.WriteString(w, err.Error())
		return
	}
	err = submitEvent2Datastore(props["mongodb.url"], props["mongodb.db"], streamName, seqid, tag, event)
	if err != nil {
		io.WriteString(w, err.Error())
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
	wsServer := wsock.NewServer(props["submitevents.uri"])
	if wsServer == nil {
		log.Fatalln("Error creating new websocket server")
	}
	go processClientConnection(wsServer, props)
	go wsServer.Listen()
	http.HandleFunc(props["postevents.uri"], handlePostRequest)
	go http.ListenAndServe(props["submitevents.url"], nil)
	err := http.ListenAndServeTLS(props["securepostevents.url"], "server.pem", "server.key", nil)
	if err != nil {
		log.Fatalln("Error while ListenAndServer", err)
	}
}
