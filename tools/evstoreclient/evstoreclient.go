//Package evstoreclient provides client to submit event to eventstore.
//

//FIXME(Vladimir): just empty bug
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"context"

	"github.com/vsysoev/goeventstore/evstore"
)

type (
	Message struct {
		stream  string
		tag     string
		payload string
	}
)

func send2EventStore(r *bufio.Reader, evStore evstore.Connection, stream string) {
	for {
		s, _, err := r.ReadLine()
		if err == io.EOF {
			break
		}
		if err != nil {
			return
		}
		fmt.Println(string(s))
		err = evStore.Committer(stream).SubmitEvent("", "scalar", string(s))
		if err != nil {
			log.Println(err)
		}
	}
	return
}
func genValue(mode map[string]interface{}) (float64, map[string]interface{}) {

	switch mode["type"].(string) {
	case "random":
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		return r.Float64() * mode["max"].(float64), mode
	case "meandr":
		if val, ok := mode["value"]; ok {
			if val == mode["min"] {
				mode["value"] = mode["max"]
			} else {
				mode["value"] = mode["min"]
			}
		} else {
			mode["value"] = mode["min"]
		}
		return mode["value"].(float64), mode
	case "saw":
		if val, ok := mode["value"]; ok {
			if val.(float64) < mode["max"].(float64) {
				mode["value"] = val.(float64) + 1
			} else {
				mode["value"] = mode["min"]
			}
		} else {
			mode["value"] = mode["min"]
		}
		return mode["value"].(float64), mode
	}
	return 0.0, nil
}
func genData(ctx context.Context, modeString string, chanOutput chan Message, stream string) {
	var (
		mode  map[string]interface{}
		value float64
	)
	err := json.Unmarshal([]byte(modeString), &mode)
	if err != nil {
		log.Println("Error in generation mode:", err, string(modeString))
		return
	}
	log.Println(mode)
	m := mode["mode"].(map[string]interface{})
	rspbox := Message{}
	rspbox.stream = "sysupdate"
	rspbox.tag = "respondbox"
	rspbox.payload = "{\"box_id\":" + strconv.FormatInt(int64(mode["box_id"].(float64)), 10) + "}"
	chanOutput <- rspbox
Loop:
	for {
		select {
		case <-ctx.Done():
			break Loop
		case <-time.After(time.Duration(mode["delay"].(float64)) * time.Millisecond):
			value, m = genValue(m)
			msg := Message{}
			msg.stream = stream + "_" + strconv.FormatInt(int64(mode["box_id"].(float64)), 10)
			msg.tag = "scalar"
			msg.payload = "{\"var_id\":" + strconv.FormatInt(int64(mode["var_id"].(float64)), 10)
			msg.payload = msg.payload + ", \"value\":" + strconv.FormatFloat(value, 'f', 5, 32) + "}"
			chanOutput <- msg
			break
		}
	}
}
func genDataFromGenFile(ctx context.Context, genFile *bufio.Reader, evStore evstore.Connection, stream string) {
	chanInput := make(chan Message, 1)
	for {
		s, _, err := genFile.ReadLine()
		if err == io.EOF {
			log.Println("EOF")
			break
		}
		if err != nil {
			log.Panicln(err)
		}
		go genData(ctx, string(s), chanInput, stream)
	}
	log.Println("Waiting for Ctrl-C")
Loop:
	for {
		select {
		case <-ctx.Done():
			break Loop
		case msg := <-chanInput:
			log.Println(msg)
			err := evStore.Committer(msg.stream).SubmitEvent("", msg.tag, msg.payload)
			if err != nil {
				log.Println(err)
			}
		}
	}
}
func main() {
	var (
		fileName       string
		uri            string
		dbName         string
		collectionName string
		msg            string
		genFile        string
	)
	flag.StringVar(&fileName, "f", "", "JSON file name with events")
	flag.StringVar(&uri, "s", "", "Event store URI")
	flag.StringVar(&dbName, "db", "", "Database name")
	flag.StringVar(&collectionName, "c", "", "Collection name")
	flag.StringVar(&msg, "msg", "", "Message to send to database")
	flag.StringVar(&genFile, "gen", "", "Genfile with information")
	flag.Parse()
	evStore, err := evstore.Dial(uri, dbName)
	if err != nil {
		log.Panicln(err)
	}
	if fileName != "" {
		if _, err = os.Stat(fileName); err != nil {
			log.Panicln(err)
		}
		f, err := os.Open(fileName)
		if err != nil {
			log.Panicln(err)
		}
		r := bufio.NewReader(f)
		send2EventStore(r, evStore, collectionName)
	}
	if msg != "" {
		err = evStore.Committer(collectionName).SubmitEvent("", "scalar", msg)
		if err != nil {
			log.Panicln(err)
		}
	}
	if genFile != "" {
		if _, err := os.Stat(genFile); err != nil {
			log.Panicln(err)
		}
		f, err := os.Open(genFile)
		if err != nil {
			log.Panicln(err)
		}
		r := bufio.NewReader(f)
		log.Println("Start GenDataFromGenFile")
		genDataFromGenFile(context.Background(), r, evStore, collectionName)
	}
	evStore.Close()
}
