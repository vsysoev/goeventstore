package evstore

import (
	"encoding/json"
	"log"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"golang.org/x/net/context"

	"gopkg.in/mgo.v2"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/vsysoev/goeventstore/property"
)

const mongoURL string = "mongodb://127.0.0.1"

type (
	PositiveListenner struct{}
)

func dropTestDatabase(dbName string) error {
	props := property.Init()
	session, err := mgo.Dial(props["mongodb.url"])
	if err != nil {
		return err
	}
	err = session.DB(dbName).C("events_capped").DropCollection()
	if err != nil {
		return err
	}
	return session.DB(dbName).C("events").DropCollection()

}
func SkipTestJSONUnmarshalling(t *testing.T) {
	Convey("Simple json unmarshalling", t, func() {
		simpleJSON := "{\"name\":\"value\"}"
		var objmap interface{}
		err := json.Unmarshal([]byte(simpleJSON), &objmap)
		So(err, ShouldBeNil)
	})
}

func SkipTestMongoCollections(t *testing.T) {
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

func SkipTestEventStore(t *testing.T) {

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
func SkipTestReadingEventAfterSubmitting(t *testing.T) {
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
func TestListen2Interface(t *testing.T) {
	Convey("Check connection to database", t, func() {
		ev, err := Dial(mongoURL, "test", "events")
		So(err, ShouldBeNil)
		So(ev, ShouldNotBeNil)
		err = ev.Listenner2().Subscribe2("scalar", sampleHandler)
		So(err, ShouldBeNil)
		ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
		go ev.Listenner2().Listen(ctx, "")
		<-ctx.Done()
		ev.Listenner2().Unsubscribe2("scalar")
		ev.Close()
	})
	Convey("When do panic in handler should not panic", t, func() {
		ev, err := Dial(mongoURL, "test", "events")
		So(err, ShouldBeNil)
		So(ev, ShouldNotBeNil)
		err = ev.Listenner2().Subscribe2("scalar", panicHandler)
		So(err, ShouldBeNil)
		ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
		go ev.Listenner2().Listen(ctx, "")
		<-ctx.Done()
		ev.Listenner2().Unsubscribe2("scalar")
		ev.Close()
	})
	Convey("Check if LastId isn't empty string", t, func() {
		dropTestDatabase("test")
		ev, err := Dial(mongoURL, "test", "events")
		So(err, ShouldBeNil)
		So(ev, ShouldNotBeNil)
		ev.Committer().SubmitEvent("", "fake", "{\"event\":\"fake\"}")
		So(err, ShouldBeNil)
		id := ev.Listenner2().GetLastId()
		So(id, ShouldNotEqual, "")
		ev.Close()

	})
}
func sampleHandler(ctx context.Context, events []interface{}) {
	for _, event := range events {
		log.Println(event)
	}

	return
}
func panicHandler(ctx context.Context, event []interface{}) {
	panic("panic Handler fired")
}
