package main

import (
	"testing"
	"time"

	"golang.org/x/net/context"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/vsysoev/goeventstore/evstore"
	"github.com/vsysoev/goeventstore/property"
	"github.com/vsysoev/goeventstore/wsock"
)

type (
	stubClient struct {
		fromWS chan *wsock.MessageT
		toWS   chan *wsock.MessageT
		doneCh chan bool
	}
)

func makeStubClient() Connector {
	s := stubClient{}
	s.fromWS = make(chan *wsock.MessageT, 128)
	s.toWS = make(chan *wsock.MessageT, 128)
	s.doneCh = make(chan bool, 128)
	return &s
}

// GetChannels stub to return channels
func (ws *stubClient) GetChannels() (chan *wsock.MessageT, chan *wsock.MessageT, chan bool) {
	return ws.fromWS, ws.toWS, ws.doneCh
}

func TestDropDatabase(t *testing.T) {
	Convey("Before start testing we drop database", t, func() {
		props := property.Init()
		session, err := mgo.Dial(props["mongodb.url"])
		So(err, ShouldBeNil)
		err = session.DB("test").DropDatabase()
		So(err, ShouldBeNil)

	})
}

//DONE:60 This test should be rewritten to submit event through websocket.
func TestSubmitEvent(t *testing.T) {
	Convey("Submit simple event", t, func() {
		var (
			results []interface{}
		)
		props := property.Init()
		ev, err := evstore.Dial(props["mongodb.url"], "test", props["mongodb.stream"])
		So(err, ShouldBeNil)
		So(ev, ShouldNotBeNil)
		c := makeStubClient()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		fromWS, _, _ := c.GetChannels()
		go handleClientRequest(ctx, c, ev)
		//		<-time.After(500 * time.Millisecond)
		m := wsock.MessageT{}
		m["sequenceid"] = ""
		m["tag"] = "test"
		m["event"] = "{\"event\":\"fake\"}"
		fromWS <- &m
		<-time.After(time.Millisecond * 100)
		session, err := mgo.Dial(props["mongodb.url"])
		So(err, ShouldBeNil)
		So(session, ShouldNotBeNil)
		iter := session.DB("test").C(props["mongodb.stream"]).Find(nil).Iter()
		So(iter, ShouldNotBeNil)

		iter.All(&results)
		So(results, ShouldNotBeNil)
		for _, v := range results {
			So(v.(bson.M)["event"].(bson.M)["event"], ShouldEqual, "fake")
		}
	})
}
