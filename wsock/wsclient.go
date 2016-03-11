package wsock

import (
	"fmt"
	"io"
	"log"
	"time"

	"golang.org/x/net/websocket"
)

const (
	channelBufSize = 100
	timeout        = time.Millisecond * 100
)

var maxID int

type (
	// Client struct holds client connection information
	Client struct {
		id     int
		ws     *websocket.Conn
		server *Server
		fromWS chan *MessageT
		toWS   chan *MessageT
		doneCh chan bool
	}
	// MessageT defines message type
	MessageT map[string]interface{}
	// Producer defines interface for client production. Servers should
	// implement this functions
	Producer interface {
		NewClient(ws *websocket.Conn, server *Server) *Client
	}
)

// NewClient creates new websocket client.
func NewClient(ws *websocket.Conn, server *Server) *Client {

	if ws == nil {
		panic("Error ws can't be nil")
	}

	if server == nil {
		panic("It isn't possible to be server as nil")
	}

	maxID++
	fromWS := make(chan *MessageT, channelBufSize)
	toWS := make(chan *MessageT, channelBufSize)
	doneCh := make(chan bool)

	return &Client{maxID, ws, server, fromWS, toWS, doneCh}
}

// GetChannels returns channels to communicate with socket
func (c *Client) GetChannels() (chan *MessageT, chan *MessageT, chan bool) {
	log.Println("In GetChannels")
	return c.fromWS, c.toWS, c.doneCh
}

// Conn return connection object
func (c *Client) Conn() *websocket.Conn {
	return c.ws
}

func (c *Client) Write(msg *MessageT) {
	select {
	case c.fromWS <- msg:
	default:
		c.server.Del(c)
		err := fmt.Errorf("client %d is disconnected ", c.id)
		c.server.Err(err)
	}
}

// Done sends done signal to done channel
func (c *Client) Done() {
	log.Println("Client.Done send doneCh")
	c.doneCh <- true
	log.Println("Client.Done Close client socket", c.ws)
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
		case <-c.doneCh:
			log.Println("listenWrite doneCh signaled")
			//			c.server.Del(c)
			c.doneCh <- true
			return

		// send message to the client
		case msg := <-c.toWS:
			websocket.JSON.Send(c.ws, msg)
			log.Println("Msg sent to websocket")
			break
		// receive done request
		case <-time.After(timeout):
		}
	}
}

func (c *Client) listenRead() {
	log.Println("Listening read from client")
	for {
		select {

		case <-c.doneCh:
			log.Println("listenRead doneCh signaled")
			c.server.Del(c)
			c.doneCh <- true
			return

		// read data from websocket connection
		default:
			var msg MessageT
			err := websocket.JSON.Receive(c.ws, &msg)
			if err == io.EOF {
				log.Println("Socket closed. Send doneCh")
				c.server.Del(c)
				c.doneCh <- true
				log.Println("Exit listenRead")
				return
			} else if err != nil {
				log.Println("error returned during parsing")
				c.server.Err(err)
				c.server.Del(c)
				c.doneCh <- true
			} else {
				log.Println("Message recieved", msg)
				c.fromWS <- &msg
			}
		}
	}
}
