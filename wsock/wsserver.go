package wsock

import (
	"log"
	"net/http"

	"golang.org/x/net/websocket"
)

// Server holds server properties
type (
	Server struct {
		pattern string
		clients map[int]*Client
		addCh   chan *Client
		delCh   chan *Client
		doneCh  chan bool
		errCh   chan error
	}
	// Handler interface defines the function which will called to process request
	Handler interface {
		ClientHandler()
	}
)

// NewServer is the server factory
func NewServer(pattern string) *Server {
	clients := make(map[int]*Client)
	addCh := make(chan *Client)
	delCh := make(chan *Client)
	doneCh := make(chan bool)
	errCh := make(chan error)
	return &Server{
		pattern,
		clients,
		addCh,
		delCh,
		doneCh,
		errCh}
}

// Add adds new client to server
func (s *Server) Add(c *Client) {
	s.addCh <- c
	log.Println("Sent client to ADD channel", s.addCh, c)
}

// Del remove client when it disconnected
func (s *Server) Del(c *Client) {
	s.delCh <- c
}

// Done indicates that server is stopping now
func (s *Server) Done() {
	s.doneCh <- true
}

// Err indicates that server in error state
func (s *Server) Err(err error) {
	s.errCh <- err
}

// GetChannels return server channel to catch client connection and client
// information from outside
func (s *Server) GetChannels() (chan *Client, chan *Client, chan bool, chan error) {
	return s.addCh, s.delCh, s.doneCh, s.errCh
}

// Listen  implements main server function
func (s *Server) Listen() {

	log.Println("Listening server...")

	// websocket handler
	onConnected := func(ws *websocket.Conn) {
		defer func() {
			log.Println("Defered socket close executed")
			err := ws.Close()
			if err != nil {
				s.errCh <- err
			}
		}()
		client := NewClient(ws, s)
		log.Println("Client connected. ", client)
		s.Add(client)
		client.Listen()
	}
	http.Handle(s.pattern, websocket.Handler(onConnected))
	log.Println("Created handler")

	for {
		select {

		// Add new a client
		/*
			case c := <-s.addCh:
				log.Println("Added new client")
				s.clients[c.id] = c
				log.Println("Now", len(s.clients), "clients connected.")

			// del a client
			case c := <-s.delCh:
				log.Println("Delete client")
				delete(s.clients, c.id)
		*/
		case err := <-s.errCh:
			log.Println("Error:", err.Error())

		case <-s.doneCh:
			return
		}
	}
}
