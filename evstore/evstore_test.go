package evstore

import (
	"encoding/json"
	"log"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"gopkg.in/mgo.v2"

	. "github.com/smartystreets/goconvey/convey"
)

const mongoURL string = "mongodb://127.0.0.1"

type (
	PositiveListenner struct{}
)

func TestJSONUnmarshalling(t *testing.T) {
	Convey("Simple json unmarshalling", t, func() {
		simpleJSON := "{\"name\":\"value\"}"
		var objmap interface{}
		err := json.Unmarshal([]byte(simpleJSON), &objmap)
		So(err, ShouldBeNil)
	})
}

func TestMongoCollections(t *testing.T) {
	Convey("Test if CollectionNames() returns not empty string array", t, func() {
		mgoSession, err := mgo.Dial(mongoURL)
		So(err, ShouldBeNil)
		So(mgoSession, ShouldNotBeNil)
		cInfo := mgo.CollectionInfo{
			Capped:   true,
			MaxBytes: 1000000,
		}
		mgoSession.DB("test").C("test_capped").Create(&cInfo)
		collections, err := mgoSession.DB("test").CollectionNames()
		So(err, ShouldBeNil)
		So(len(collections), ShouldBeGreaterThan, 0)
		bFound := false
		for _, s := range collections {
			if s == "test_capped" {
				bFound = true
			}
		}
		So(bFound, ShouldBeTrue)
		mgoSession.DB("test").C("test_capped").DropCollection()
	})
}

func TestEventStore(t *testing.T) {

	Convey("Test if contains works", t, func() {
		arr := []string{"value1", "value2", "value3"}
		So(contains(arr, "value1"), ShouldBeTrue)
		So(contains(arr, "value3"), ShouldBeTrue)
		So(contains(arr, "value5"), ShouldBeFalse)
	})
	Convey("Test MongoEventReader", t, func() {
		mng, err := Dial(mongoURL, "test", "events")
		So(mng, ShouldNotBeNil)
		So(err, ShouldBeNil)
	})
	Convey("Test CommitEvent of MongoEventWriter", t, func() {
		mng, err := Dial(mongoURL, "test", "events")
		So(mng, ShouldNotBeNil)
		So(err, ShouldBeNil)
		err = mng.Committer().SubmitEvent("", "test", "{\"test\":\"value\"}")
		So(err, ShouldBeNil)
		err = mng.Committer().SubmitEvent("", "test", "{\"array\":[\"123\",\"345\", 45, 3445.456]}")
		So(err, ShouldBeNil)
	})

	Convey("ReadEvents from the database", t, func() {
		mng, err := Dial(mongoURL, "test", "events")
		So(mng, ShouldNotBeNil)
		So(err, ShouldBeNil)
		ch, err := mng.Listenner().Subscribe("")
		So(err, ShouldBeNil)
		var lastID string
	Loop:
		for {
			select {
			case s := <-ch:
				So(s, ShouldNotEqual, "")
				var js map[string]interface{}
				e := json.Unmarshal([]byte(s), &js)
				So(e, ShouldBeNil)
				lastID = js["_id"].(string)
				So(lastID, ShouldNotEqual, "")
				break
			default:
				log.Println("Breaking loop")
				break Loop
			}
		}
		log.Println("Before mng.Close()")
		mng.Close()
	})

	Convey("Close connection", t, func() {
		mng, err := Dial(mongoURL, "test", "events")
		So(mng, ShouldNotBeNil)
		So(err, ShouldBeNil)
		ch, err := mng.Listenner().Subscribe("")
		So(err, ShouldBeNil)
		So(ch, ShouldNotBeNil)
		mng.Close()
	})

}
func TestReadingEventAfterSubmitting(t *testing.T) {
	Convey("When commit current message to database", t, func() {
		ev, err := Dial(mongoURL, "test", "events")
		So(err, ShouldBeNil)
		So(ev, ShouldNotBeNil)

		ch, err := ev.Listenner().Subscribe("")
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
		log.Println("All messages read")
		for i := 1; i < 10; i++ {
			timestamp := strconv.FormatInt(time.Now().Unix(), 10)
			log.Println(timestamp)
			rand.Seed(time.Now().UTC().UnixNano())
			boxID := strconv.Itoa(rand.Intn(100))
			varID := strconv.Itoa(rand.Intn(200))
			val := strconv.FormatFloat(rand.NormFloat64(), 'f', 2, 64)
			sendMsg := "{\"datestamp\":\"" + timestamp + "\", \"box_id\": " + boxID + ", \"var_id\": " + varID + ", \"value\": " + val + "}"
			err := ev.Committer().SubmitEvent("123", "scalar", sendMsg)
			So(err, ShouldBeNil)
			msg := ""
			select {
			case msg = <-ch:
				log.Println("Cought last message", msg)
				break
			case <-time.After(time.Second * 1):
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
