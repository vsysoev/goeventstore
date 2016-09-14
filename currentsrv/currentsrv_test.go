package main

import (
	"errors"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/websocket"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"context"

	"github.com/docker/docker/pkg/pubsub"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/vsysoev/goeventstore/evstore"
	"github.com/vsysoev/goeventstore/property"
	"github.com/vsysoev/goeventstore/wsock"
)

const (
	mongoURL string = "mongodb://127.0.0.1"
	dbName   string = "currentsrv_test"
)

type (
	stubServer struct {
		addCh  chan *wsock.ClientInterface
		delCh  chan *wsock.ClientInterface
		doneCh chan bool
		errCh  chan error
	}
	nilServer struct{}
	nilClient struct{}
)

var (
	msgNumber int
)

func newNilClient() wsock.ClientInterface {
	return &nilClient{}
}
func (c *nilClient) GetChannels() (chan *wsock.MessageT, chan *wsock.MessageT, chan bool) {
	return nil, nil, nil
}
func (c *nilClient) Request() *http.Request {
	return nil
}
func (c *nilClient) Conn() *websocket.Conn {
	return nil
}
func (c *nilClient) Write(msg *wsock.MessageT) {

}
func (c *nilClient) Done()   {}
func (c *nilClient) Listen() {}

func newNilServer() wsock.ServerInterface {
	return &nilServer{}
}

// Add adds new client to server
func (s *nilServer) Add(c wsock.ClientInterface) {
	log.Println("Add client", c)
}

// Del remove client when it disconnected
func (s *nilServer) Del(c wsock.ClientInterface) {
	log.Println("Del client ", c)
}

// Done indicates that server is stopping now
func (s *nilServer) Done() {
	log.Println("Done true")
}

// Err indicates that server in error state
func (s *nilServer) Err(err error) {
	log.Println(err)
}

// GetChannels return server channel to catch client connection and client
// information from outside
func (s *nilServer) GetChannels() (chan *wsock.ClientInterface, chan *wsock.ClientInterface, chan bool, chan error) {
	return nil, nil, nil, nil
}

// Listen  implements main server function
func (s *nilServer) Listen() {

	log.Println("stubServer Listen for 2 seconds...")
	<-time.After(2 * time.Second)
}

func newStubServer() wsock.ServerInterface {
	aC := make(chan *wsock.ClientInterface)
	dC := make(chan *wsock.ClientInterface)
	doneCh := make(chan bool)
	errCh := make(chan error)
	return &stubServer{addCh: aC, delCh: dC, doneCh: doneCh, errCh: errCh}
}

// Add adds new client to server
func (s *stubServer) Add(c wsock.ClientInterface) {
	log.Println("Add client", c)
}

// Del remove client when it disconnected
func (s *stubServer) Del(c wsock.ClientInterface) {
	log.Println("Del client ", c)
}

// Done indicates that server is stopping now
func (s *stubServer) Done() {
	log.Println("Done true")
}

// Err indicates that server in error state
func (s *stubServer) Err(err error) {
	log.Println(err)
}

// GetChannels return server channel to catch client connection and client
// information from outside
func (s *stubServer) GetChannels() (chan *wsock.ClientInterface, chan *wsock.ClientInterface, chan bool, chan error) {
	return s.addCh, s.delCh, s.doneCh, s.errCh
}

// Listen  implements main server function
func (s *stubServer) Listen() {

	log.Println("stubServer Listen for 2 seconds...")
	<-time.After(2 * time.Second)
}

func Handler(ctx context.Context, stream string, msgs []interface{}) {
	log.Println("Enter handler")
	log.Println(msgs)
	msgNumber = msgNumber - len(msgs)
}

func initEventStore(url string, dbName string) (evstore.Connection, error) {
	ev, err := evstore.Dial(mongoURL, dbName)
	if err != nil {
		return nil, err
	}
	err = ev.Manager().DropDatabase(dbName)
	return ev, err
}
func TestDropDatabase(t *testing.T) {
	Convey("Before start testing we drop database", t, func() {
		props := property.Init()
		session, err := mgo.Dial(props["mongodb.url"])
		So(session, ShouldNotBeNil)
		So(err, ShouldBeNil)
		_ = session.DB(dbName).C("events_capped").DropCollection()
		_ = session.DB(dbName).C("events").DropCollection()
	})
}

func respondBoxHandler(ctx context.Context, stream string, msgs []interface{}) {
	msgNumber = msgNumber - len(msgs)
}
func TestRespondBoxState(t *testing.T) {
	Convey("When i commit respondbox message", t, func() {
		ev, err := initEventStore(mongoURL, dbName)
		So(err, ShouldBeNil)
		So(ev, ShouldNotBeNil)
		ctx, cancel := context.WithCancel(context.Background())
		err = ev.Listenner2().Subscribe2("systemupdate", "respondbox", "", respondBoxHandler)
		defer cancel()
		go ev.Listenner2().Listen(ctx, "")
		So(err, ShouldBeNil)
		msgNumber = 9
		for i := 1; i < 10; i++ {
			boxID := strconv.Itoa(i)
			sendMsg := "{\"box_id\":" + boxID + "}"
			err = ev.Committer("systemupdate").SubmitEvent("", "respondbox", sendMsg)
			So(err, ShouldBeNil)
		}
		<-time.After(1 * time.Second)
		So(msgNumber, ShouldEqual, 0)
	})
}
func TestCurrentScalarState(t *testing.T) {
	Convey("When commit current message to database", t, func() {
		ev, err := initEventStore(mongoURL, dbName)
		So(err, ShouldBeNil)
		So(ev, ShouldNotBeNil)

		err = ev.Listenner2().Subscribe2("scalar_1", "scalar", "", Handler)
		So(err, ShouldBeNil)
		sendMsg := "{\"box_id\":1}"
		err = ev.Committer("systemupdate").SubmitEvent("", "respondbox", sendMsg)
		So(err, ShouldBeNil)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go ev.Listenner2().Listen(ctx, "")
		msgNumber = 9
		for i := 1; i < 10; i++ {
			varID := strconv.Itoa(rand.Intn(200))
			val := strconv.FormatFloat(rand.NormFloat64(), 'f', 2, 64)
			sendMsg := "{\"var_id\":" + varID + ", \"value\": " + val + "}"
			err = ev.Committer("scalar_1").SubmitEvent("123", "scalar", sendMsg)
			So(err, ShouldBeNil)
		}
		<-time.After(1 * time.Second)
		So(msgNumber, ShouldEqual, 0)
		ev.Close()
	})
}

func TestScalarHandler(t *testing.T) {
	Convey("When pass empty message slice to scalarHandler", t, func() {
		var msgs []interface{}
		Convey("It should not panic", func() {
			So(func() { scalarHandler(context.Background(), "", msgs) }, ShouldNotPanic)
		})
	})
	Convey("When pass message with int boxID, varID", t, func() {
		var msgs []interface{}
		sState = ScalarState{}
		sState.state = make(map[int]map[int]*bson.M)
		sState.mx = &sync.Mutex{}
		stateUpdateChannel := pubsub.NewPublisher(time.Millisecond*100, 1024)
		ctx := context.WithValue(context.Background(), "stateUpdate", stateUpdateChannel)
		id := bson.NewObjectId()
		msg := bson.M{"_id": id, "tag": "scalar", "event": bson.M{"box_id": 1, "var_id": 1, "value": 1.5}}
		msgs = append(msgs, msg)
		Convey("It should not panic with type assertion", func() {
			So(func() { scalarHandler(ctx, "", msgs) }, ShouldNotPanic)
		})
	})
	Convey("When pass message with float64 boxID, varID", t, func() {
		var msgs []interface{}
		sState = ScalarState{}
		sState.state = make(map[int]map[int]*bson.M)
		sState.mx = &sync.Mutex{}
		stateUpdateChannel := pubsub.NewPublisher(time.Millisecond*100, 1024)
		ctx := context.WithValue(context.Background(), "stateUpdate", stateUpdateChannel)
		id := bson.NewObjectId()
		msg := bson.M{"_id": id, "tag": "scalar", "event": bson.M{"box_id": 1.0, "var_id": 1.0, "value": 1.5}}
		msgs = append(msgs, msg)
		Convey("It should not panic with type assertion", func() {
			So(func() { scalarHandler(ctx, "stateUpdate", msgs) }, ShouldNotPanic)
		})
	})
	Convey("When pass message with string boxID, varID", t, func() {
		var msgs []interface{}
		sState = ScalarState{}
		sState.state = make(map[int]map[int]*bson.M)
		sState.mx = &sync.Mutex{}
		stateUpdateChannel := make(chan *bson.M, 256)
		ctx := context.WithValue(context.Background(), "stateUpdateChannel", stateUpdateChannel)
		id := bson.NewObjectId()
		msg := bson.M{"_id": id, "tag": "scalar", "event": bson.M{"box_id": "1.0", "var_id": "1.0", "value": 1.5}}
		msgs = append(msgs, msg)
		Convey("It should not panic with type assertion", func() {
			So(func() { scalarHandler(ctx, "scalar", msgs) }, ShouldNotPanic)
		})
	})
	Convey("When pass message with int boxID and string varID", t, func() {
		var msgs []interface{}
		sState = ScalarState{}
		sState.state = make(map[int]map[int]*bson.M)
		sState.mx = &sync.Mutex{}
		stateUpdateChannel := make(chan *bson.M, 256)
		ctx := context.WithValue(context.Background(), "stateUpdateChannel", stateUpdateChannel)
		id := bson.NewObjectId()
		msg := bson.M{"_id": id, "tag": "scalar", "event": bson.M{"box_id": 1, "var_id": "1.0", "value": 1.5}}
		msgs = append(msgs, msg)
		Convey("It should not panic with type assertion", func() {
			So(func() { scalarHandler(ctx, "", msgs) }, ShouldNotPanic)
		})
	})
}

func TestSystemUpdateHandler(t *testing.T) {
	Convey("When pass empty message slice to systemUpdateHandler", t, func() {
		var msgs []interface{}
		Convey("It should not panic", func() {
			So(func() { systemUpdateHandler(context.Background(), "", msgs) }, ShouldNotPanic)
		})
	})
	Convey("When EventStore isn't defined", t, func() {
		var msgs []interface{}
		ctx := context.Background()
		id := bson.NewObjectId()
		msg := bson.M{"_id": id, "tag": "respondbox", "event": bson.M{"box_id": 1}}
		msgs = append(msgs, msg)
		Convey("It should not be panic", func() {
			So(func() { systemUpdateHandler(ctx, "sysupdate", msgs) }, ShouldNotPanic)
		})
	})
	Convey("When respondbox sent", t, func() {
		ev, err := initEventStore(mongoURL, dbName)
		So(err, ShouldBeNil)
		So(ev, ShouldNotBeNil)
		var msgs []interface{}
		ctx := context.WithValue(context.Background(), "evStore", ev)
		id := bson.NewObjectId()
		msg := bson.M{"_id": id, "tag": "respondbox", "event": bson.M{"box_id": 1}}
		msgs = append(msgs, msg)
		Convey("It should not panic with type assertion", func() {
			So(func() { systemUpdateHandler(ctx, "sysupdate", msgs) }, ShouldNotPanic)
		})
	})
}
func TestProcessClientConnection(t *testing.T) {
	Convey("When just call processClientConnection", t, func() {
		ctx := context.Background()
		s := newNilServer()

		Convey("It should not panic", func() {
			So(func() { processClientConnection(ctx, s) }, ShouldNotPanic)
		})
	})
	Convey("When call processClientConnection with stubServer", t, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		s := newStubServer()
		_, _, doneC, _ := s.GetChannels()
		Convey("It should exit", func() {
			go processClientConnection(ctx, s)
			<-time.After(1 * time.Second)
			doneC <- true
		})
	})
	Convey("When call processClientConnection with context.Timeout", t, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		s := newStubServer()
		Convey("It should exit", func() {
			go processClientConnection(ctx, s)
			<-time.After(2 * time.Second)
		})
	})
	Convey("When send error to processClientConnection", t, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		s := newStubServer()
		_, _, _, errC := s.GetChannels()
		Convey("It should exit", func() {
			go processClientConnection(ctx, s)
			<-time.After(1 * time.Second)
			errC <- errors.New("Fake error message")
		})
	})
	Convey("When send add client to processClientConnection", t, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		stateUpdate := pubsub.NewPublisher(time.Millisecond*100, 1024)
		ctx1 := context.WithValue(ctx, "stateUpdate", stateUpdate)
		s := newStubServer()
		addC, _, _, _ := s.GetChannels()
		Convey("It should exit", func() {
			go processClientConnection(ctx1, s)
			<-time.After(1 * time.Second)
			c := newNilClient()
			addC <- &c
		})
	})
	Convey("Send add/remove client to processClientConnection", t, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		stateUpdate := pubsub.NewPublisher(time.Millisecond*100, 1024)
		ctx1 := context.WithValue(ctx, "stateUpdate", stateUpdate)
		s := newStubServer()
		addC, delC, _, _ := s.GetChannels()
		go processClientConnection(ctx1, s)
		<-time.After(100 * time.Millisecond)
		c := newNilClient()
		addC <- &c
		<-time.After(100 * time.Millisecond)
		delC <- &c
		<-time.After(100 * time.Millisecond)
	})
}
func TestHandleClient(t *testing.T) {
	Convey("When filter is string", t, func() {
		ctx := context.Background()
		cli := newNilClient()
		ctx1 := context.WithValue(ctx, "client", &cli)
		Convey("It should not panic", func() {
			So(func() { handleClient(ctx1) }, ShouldNotPanic)
		})
	})
}
