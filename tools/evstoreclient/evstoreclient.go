package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/vsysoev/goeventstore/evstore"
)

func Send2EventStore(r *bufio.Reader, evStore *evstore.Connection) {
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
func main() {
	var (
		fileName       string
		uri            string
		dbName         string
		collectionName string
		msg            string
	)
	flag.StringVar(&fileName, "f", "", "JSON file name with events")
	flag.StringVar(&uri, "s", "", "Event store URI")
	flag.StringVar(&dbName, "db", "", "Database name")
	flag.StringVar(&collectionName, "c", "", "Collection name")
	flag.StringVar(&msg, "msg", "", "Message to send to database")
	flag.Parse()
	evStore, err := evstore.Dial(uri, dbName, collectionName)
	if err != nil {
		log.Panicln(err)
	}
	if fileName != "" {
		if _, err := os.Stat(fileName); err != nil {
			log.Panicln(err)
		}
		f, err := os.Open(fileName)
		if err != nil {
			log.Panicln(err)
		}
		r := bufio.NewReader(f)
		Send2EventStore(r, evStore)
	}
	if msg != "" {
		err = evStore.Committer().SubmitEvent("", "scalar", msg)
		if err != nil {
			log.Panicln(err)
		}
	}
	evStore.Close()
}
