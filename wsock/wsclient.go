package wsock

import (
	"fmt"
	"io"
	"log"

	"golang.org/x/net/websocket"
)

const channelBufSize = 100

var maxId int = 0

// Clients struct holds client connection information
type Client struct {
	id     int
	ws     *websocket.Conn
	server *Server
	ch     chan string
	cmdCh  chan map[string]interface{}
	doneCh chan bool
}

// NewClient creates new websocket client.
func NewClient(ws *websocket.Conn, server *Server) *Client {

	if ws == nil {
		panic("Error ws can't be nil")
	}

	if server == nil {
		panic("It isn't possible to be server as nil")
	}

	maxId++
	ch := make(chan string, channelBufSize)
	doneCh := make(chan bool)
	cmdCh := make(chan map[string]interface{}, channelBufSize)

	return &Client{maxId, ws, server, ch, cmdCh, doneCh}
}

func (c *Client) Conn() *websocket.Conn {
	return c.ws
}

func (c *Client) Write(msg string) {
	select {
	case c.ch <- msg:
	default:
		c.server.Del(c)
		err := fmt.Errorf("client %d is disconnected.", c.id)
		c.server.Err(err)
	}
}

func (c *Client) Done() {
	c.doneCh <- true
}

// Listen Write and Read request via chanel
func (c *Client) Listen() {
	go c.listenWrite()
	c.listenRead()
}

func (c *Client) listenWrite() {
	log.Println("Listening write to client")
	for {
		select {

		// send message to the client
		case msg := <-c.ch:
			log.Println("Send:", msg)
			websocket.JSON.Send(c.ws, msg)

		// receive done request
		case <-c.doneCh:
			c.server.Del(c)
			c.doneCh <- true
			return
		}
	}
}

func (c *Client) listenRead() {
	log.Println("Listening read from client")
	for {
		select {

		case <-c.doneCh:
			c.server.Del(c)
			c.doneCh <- true
			return

		// read data from websocket connection
		default:
			var msg map[string]interface{}
			err := websocket.JSON.Receive(c.ws, &msg)
			if err == io.EOF {
				c.doneCh <- true
			} else if err != nil {
				c.server.Err(err)
			} else {
				log.Println("Message recieved", msg)
				c.cmdCh <- msg
			}
		}
	}
}
