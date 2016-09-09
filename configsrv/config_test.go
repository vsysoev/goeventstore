package main

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"gopkg.in/mgo.v2"

	"context"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/vsysoev/goeventstore/evstore"
	"github.com/vsysoev/goeventstore/property"
)

const (
	mongoURL string = "mongodb://127.0.0.1"
)

func Handler(ctx context.Context, msgs []interface{}) {

}
func TestDropDatabase(t *testing.T) {
	Convey("Before start testing we drop database", t, func() {
		props := property.Init()
		session, err := mgo.Dial(props["mongodb.url"])
		So(session, ShouldNotBeNil)
		So(err, ShouldBeNil)
		_ = session.DB("test").C("events_capped").DropCollection()
		_ = session.DB("test").C("events").DropCollection()
	})
}
func TestCurrentScalarState(t *testing.T) {
	Convey("When commit current message to database", t, func() {
		ev, err := evstore.Dial(mongoURL, "test")
		So(err, ShouldBeNil)
		So(ev, ShouldNotBeNil)

		err = ev.Listenner2("events").Subscribe2("", Handler)
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
			err = ev.Committer("events").SubmitEvent("123", "scalar", sendMsg)
			So(err, ShouldBeNil)
		}
		ev.Close()
	})
}
