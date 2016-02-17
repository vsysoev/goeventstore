package main

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"golang.org/x/net/context"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/vsysoev/goeventstore/evstore"
)

const (
	mongoURL string = "mongodb://127.0.0.1"
)

func handlerTest(ctx context.Context, events []interface{}) {
	for _, ev := range events {
		log.Println(ev)
		//		ctx.Value("id") = ev.(bson.M)["_id"].(bson.ObjectId).Hex()
	}
}
func TestEventSrv(t *testing.T) {
	Convey("When commit current message to database", t, func() {
		ev, err := evstore.Dial(mongoURL, "test", "events")
		So(err, ShouldBeNil)
		So(ev, ShouldNotBeNil)

		err = ev.Listenner2().Subscribe2("", handlerTest)
		So(err, ShouldBeNil)
		ctx := context.WithValue(context.Background(), "test", t)
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		go ev.Listenner2().Listen(ctx, "")
		<-time.After(time.Second * 2)

		fmt.Println("All messages read")
		for i := 0; i < 10; i++ {
			timestamp := strconv.FormatInt(time.Now().Unix(), 10)
			log.Println(timestamp)
			rand.Seed(time.Now().UTC().UnixNano())
			boxID := strconv.Itoa(rand.Intn(100))
			varID := strconv.Itoa(rand.Intn(200))
			val := strconv.FormatFloat(rand.NormFloat64(), 'f', 2, 64)
			sendMsg := "{\"datestamp\":\"" + timestamp + "\", \"box_id\": " + boxID + ", \"var_id\": " + varID + ", \"value\": " + val + "}"
			err = ev.Committer().SubmitEvent("123", "scalar", sendMsg)
			So(err, ShouldBeNil)
		}
		<-time.After(time.Second * 3)
		ev.Close()
	})
}
