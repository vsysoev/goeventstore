package main

import (
	"flag"
	"io"
	"log"
	"sync"
	"time"

	"golang.org/x/net/websocket"
)

func cli(server string, msg string, wg *sync.WaitGroup) {
	defer wg.Done()
	origin := "http://localhost"
	ws, err := websocket.Dial(server, "", origin)
	if err != nil {
		panic(err)
	}
	_, err = ws.Write([]byte(msg))
	if err != nil {
		panic(err)
	}

	ws.SetReadDeadline(time.Now().Add(time.Second * 3))
	msgOut := make([]byte, 1024)
	for {
		n, err := ws.Read(msgOut)
		if err == io.EOF {
			log.Println("Reading finished")
			break
		}
		if err != nil {
			log.Println(err)
			break
		}
		log.Println(string(msgOut[:n]))
	}
	ws.Close()
}

func main() {
	var (
		server       string
		clientNumber int
		msg          string
	)
	flag.StringVar(&server, "ws", "ws://127.0.0.1:8899/ws", "Address to WS Server")
	flag.IntVar(&clientNumber, "n", 1, "Number of simultaneous client")
	flag.StringVar(&msg, "m", "{\"get\":{\"id\":\"\"}}", "Message to send")
	flag.Parse()
	if !flag.Parsed() {
		flag.PrintDefaults()
		return
	}
	wg := sync.WaitGroup{}
	for i := 0; i < clientNumber; i++ {
		wg.Add(1)
		go cli(server, msg, &wg)
	}
	wg.Wait()
}
