package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"os"

	"github.com/vsysoev/goeventstore/evstore"
)

func Send2EventStore(jsonString []byte, evStore *evstore.Connection) {
	js := make(map[string]interface{})
	err := json.Unmarshal(jsonString, &js)
	if err != nil {
		log.Panicln(err)
		return
	}
	return
}
func main() {
	var (
		fileName       string
		uri            string
		dbName         string
		collectionName string
	)
	flag.StringVar(&fileName, "f", "", "JSON file name with events")
	flag.StringVar(&uri, "s", "", "Event store URI")
	flag.StringVar(&dbName, "db", "", "Database name")
	flag.StringVar(&collectionName, "c", "", "Collection name")
	flag.Parse()
	if _, err := os.Stat(fileName); err != nil {
		log.Panicln(err)
	}
	jsonStrings, err := ioutil.ReadFile(fileName)
	if err != nil {
		log.Panicln(err)
	}
	evStore, err := evstore.Dial(uri, dbName, collectionName)
	if err != nil {
		log.Panicln(err)
	}
	Send2EventStore(jsonStrings, evStore)
	evStore.Close()
}
