package main

import (
	"log"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"context"

	"github.com/docker/docker/pkg/pubsub"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/vsysoev/goeventstore/evstore"
	"github.com/vsysoev/goeventstore/property"
)

const (
	mongoURL string = "mongodb://127.0.0.1"
	dbName   string = "currentsrv_test"
)

var (
	msgNumber int
)

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
			So(func() { scalarHandler(context.Background(), msgs) }, ShouldNotPanic)
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
			So(func() { scalarHandler(ctx, msgs) }, ShouldNotPanic)
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
			So(func() { scalarHandler(ctx, msgs) }, ShouldNotPanic)
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
			So(func() { scalarHandler(ctx, msgs) }, ShouldNotPanic)
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
			So(func() { scalarHandler(ctx, msgs) }, ShouldNotPanic)
		})
	})
}
