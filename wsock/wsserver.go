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
		clients map[int]*ClientInterface
		addCh   chan *ClientInterface
		delCh   chan *ClientInterface
		doneCh  chan bool
		errCh   chan error
	}
	// Handler interface defines the function which will called to process request
	Handler interface {
		ClientHandler()
	}
	// ServerInterface defines Server function to support mocking for testing
	ServerInterface interface {
		Add(c ClientInterface)
		// Del remove client when it disconnected
		Del(c ClientInterface)

		// Done indicates that server is stopping now
		Done()

		// Err indicates that server in error state
		Err(err error)

		// GetChannels return server channel to catch client connection and client
		// information from outside
		GetChannels() (chan *ClientInterface, chan *ClientInterface, chan bool, chan error)

		// Listen  implements main server function
		Listen()
	}
)

// NewServer is the server factory
func NewServer(pattern string) ServerInterface {
	clients := make(map[int]*ClientInterface)
	addCh := make(chan *ClientInterface)
	delCh := make(chan *ClientInterface)
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
func (s *Server) Add(c ClientInterface) {
	s.addCh <- &c
	log.Println("Sent client to ADD channel", s.addCh, c)
}

// Del remove client when it disconnected
func (s *Server) Del(c ClientInterface) {
	s.delCh <- &c
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
func (s *Server) GetChannels() (chan *ClientInterface, chan *ClientInterface, chan bool, chan error) {
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

		case err := <-s.errCh:
			log.Println("Error:", err.Error())

		case <-s.doneCh:
			return
		}
	}
}
