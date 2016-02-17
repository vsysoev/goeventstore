package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/vsysoev/goeventstore/evstore"
)

const (
	mongoURL string = "mongodb://127.0.0.1"
)

func handlerTest(events []interface{}, ctx Context) {

}
func TestEventSrv(t *testing.T) {
	Convey("When commit current message to database", t, func() {
		ev, err := evstore.Dial(mongoURL, "test", "events")
		So(err, ShouldBeNil)
		So(ev, ShouldNotBeNil)

		ch, err := ev.Listenner2().Subscribe2("", handlerTest)
		So(ch, ShouldNotBeNil)
	Loop:
		for {
			select {
			case <-ch:
				//			fmt.Println(msg)
				break
			case <-time.After(time.Second):
				break Loop
			}
		}
		fmt.Println("All messages read")
		for i := 1; i < 10; i++ {
			timestamp := strconv.FormatInt(time.Now().Unix(), 10)
			log.Println(timestamp)
			rand.Seed(time.Now().UTC().UnixNano())
			boxID := strconv.Itoa(rand.Intn(100))
			varID := strconv.Itoa(rand.Intn(200))
			val := strconv.FormatFloat(rand.NormFloat64(), 'f', 2, 64)
			sendMsg := "{\"datestamp\":\"" + timestamp + "\", \"box_id\": " + boxID + ", \"var_id\": " + varID + ", \"value\": " + val + "}"
			err = ev.Committer().SubmitEvent("123", "scalar", sendMsg)
			So(err, ShouldBeNil)
			msg := ""
			select {
			case msg = <-ch:
				break
			case <-time.After(time.Second * 10):
				msg = "{\"event\":{\"error\": \"This is fucking shit error\"}}"
				break
			}
			var msgJSON map[string]interface{}
			err = json.Unmarshal([]byte(msg), &msgJSON)
			So(msgJSON["event"].(map[string]interface{})["datestamp"].(string), ShouldEqual, timestamp)

		}
		ev.Close()
	})
}
