package main

import (
	"bufio"
	"crypto/tls"
	"flag"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"golang.org/x/net/websocket"
)

type (
	CommandLineArguments struct {
		server       string
		msg          string
		file         string
		timeOut      int
		clientNumber int
	}
)

func cli(args CommandLineArguments, wg *sync.WaitGroup) {
	defer wg.Done()
	cfg, err := websocket.NewConfig(args.server, "wss://localhost")
	if err != nil {
		panic(err)
	}
	cert, err := tls.LoadX509KeyPair("server.pem", "server.key")
	if err != nil {
		panic(err)
	}
	config := tls.Config{Certificates: []tls.Certificate{cert}}
	config.InsecureSkipVerify = true
	cfg.TlsConfig = &config
	ws, err := websocket.DialConfig(cfg)
	if err != nil {
		panic(err)
	}
	if args.msg != "" {
		_, err = ws.Write([]byte(args.msg))
		if err != nil {
			panic(err)
		}
	}
	if args.file != "" {
		f, err := os.Open(args.file)
		if err != nil {
			panic(err)
		}
		defer f.Close()
		s := bufio.NewScanner(f)
		for s.Scan() {
			_, err = ws.Write([]byte(s.Text()))
			if err != nil {
				panic(err)
			}
		}
	}
	ws.SetReadDeadline(time.Now().Add(time.Second * time.Duration(args.timeOut)))
	msgOut := make([]byte, 16384)
	for {
		n, err := ws.Read(msgOut)
		log.Println("Read bytes:", n)
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
	log.Println("Closing connection")
	ws.Close()
}

func main() {
	var (
		args CommandLineArguments
	)
	flag.StringVar(&args.server, "ws", "ws://127.0.0.1:8899/ws", "Address to WS Server")
	flag.IntVar(&args.clientNumber, "n", 1, "Number of simultaneous client")
	flag.StringVar(&args.msg, "m", "", "Message to send")
	flag.StringVar(&args.file, "f", "", "File with json messages to send")
	flag.IntVar(&args.timeOut, "t", 3, "Timeout in seconds")
	flag.Parse()
	if !flag.Parsed() {
		flag.PrintDefaults()
		return
	}
	wg := sync.WaitGroup{}
	for i := 0; i < args.clientNumber; i++ {
		wg.Add(1)
		go cli(args, &wg)
	}
	wg.Wait()
	log.Println("All clients done. Exiting")
}
