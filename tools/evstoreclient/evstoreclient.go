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

	"golang.org/x/net/context"

	"github.com/vsysoev/goeventstore/evstore"
)

func send2EventStore(r *bufio.Reader, evStore evstore.Connection) {
	for {
		s, _, err := r.ReadLine()
		if err == io.EOF {
			break
		}
		if err != nil {
			return
		}
		fmt.Println(string(s))
		err = evStore.Committer().SubmitEvent("", "scalar", string(s))
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
func genData(ctx context.Context, modeString string, chanOutput chan string) {
	var (
		mode  map[string]interface{}
		value float64
	)
	err := json.Unmarshal([]byte(modeString), &mode)
	if err != nil {
		log.Panicln("Error in generation mode:", string(modeString))
	}
	log.Println(mode)
	m := mode["mode"].(map[string]interface{})
Loop:
	for {
		select {
		case <-ctx.Done():
			break Loop
		case <-time.After(time.Duration(mode["delay"].(float64)) * time.Millisecond):
			value, m = genValue(m)
			event := "{ \"box_id\":" + strconv.FormatInt(int64(mode["box_id"].(float64)), 10)
			event = event + ", \"var_id\":" + strconv.FormatInt(int64(mode["var_id"].(float64)), 10)
			event = event + ", \"value\":" + strconv.FormatFloat(value, 'f', 5, 32) + "}"
			chanOutput <- event
			break
		}
	}
}
func genDataFromGenFile(ctx context.Context, genFile *bufio.Reader, evStore evstore.Connection) {
	chanInput := make(chan string, 1)
	for {
		s, _, err := genFile.ReadLine()
		if err == io.EOF {
			log.Println("EOF")
			break
		}
		if err != nil {
			log.Panicln(err)
		}
		go genData(ctx, string(s), chanInput)
	}
	log.Println("Waiting for Ctrl-C")
Loop:
	for {
		select {
		case <-ctx.Done():
			break Loop
		case msg := <-chanInput:
			log.Println(msg)
			evStore.Committer().SubmitEvent("", "scalar", msg)
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
	evStore, err := evstore.Dial(uri, dbName, collectionName)
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
		send2EventStore(r, evStore)
	}
	if msg != "" {
		err = evStore.Committer().SubmitEvent("", "scalar", msg)
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
		genDataFromGenFile(context.Background(), r, evStore)
	}
	evStore.Close()
}
