package main

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"golang.org/x/net/context"

	"github.com/docker/docker/pkg/pubsub"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/vsysoev/goeventstore/evstore"
	"github.com/vsysoev/goeventstore/property"
)

const (
	mongoURL string = "mongodb://127.0.0.1"
	dbName   string = "currentsrv_test"
)

func Handler(ctx context.Context, msgs []interface{}) {

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
func TestCurrentScalarState(t *testing.T) {
	Convey("When commit current message to database", t, func() {
		ev, err := evstore.Dial(mongoURL, dbName, "events")
		So(err, ShouldBeNil)
		So(ev, ShouldNotBeNil)

		err = ev.Listenner2().Subscribe2("", Handler)
		So(err, ShouldBeNil)
		//	Loop:
		fmt.Println("All messages read")
		for i := 1; i < 10; i++ {
			timestamp := strconv.FormatInt(time.Now().UnixNano(), 10)
			log.Println(timestamp)
			rand.Seed(time.Now().UTC().UnixNano())
			boxID := strconv.Itoa(rand.Intn(100))
			varID := strconv.Itoa(rand.Intn(200))
			boxID = "1"
			if i > 4 {
				varID = "3"
			} else {
				varID = "4"
			}
			val := strconv.FormatFloat(rand.NormFloat64(), 'f', 2, 64)
			sendMsg := "{\"datestamp\":\"" + timestamp + "\", \"box_id\": " + boxID + ", \"var_id\": " + varID + ", \"value\": " + val + "}"
			err = ev.Committer().SubmitEvent("123", "scalar", sendMsg)
			So(err, ShouldBeNil)
		}
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
