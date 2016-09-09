package evstore

import (
	"encoding/json"
	"log"
	"strconv"
	"testing"
	"time"

	"context"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/vsysoev/goeventstore/property"
)

const mongoURL string = "mongodb://127.0.0.1"
const dbName string = "evstore_test"

type (
	PositiveListenner struct{}
)

func dropTestDatabase(dbName string) error {
	props := property.Init()
	session, err := mgo.Dial(props["mongodb.url"])
	if err != nil {
		return err
	}
	_ = session.DB(dbName).C("events_capped").DropCollection()
	_ = session.DB(dbName).C("events").DropCollection()
	return nil
}

func getDatabaseNames() ([]string, error) {
	session, err := mgo.Dial(mongoURL)
	if err != nil {
		return nil, err
	}
	return session.DatabaseNames()
}
func TestListen2Interface(t *testing.T) {
	Convey("Check connection to database", t, func() {
		ev, err := Dial(mongoURL, dbName)
		So(err, ShouldBeNil)
		So(ev, ShouldNotBeNil)
		err = ev.Listenner2("events").Subscribe2("scalar", sampleHandler)
		So(err, ShouldBeNil)
		ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
		go ev.Listenner2("events").Listen(ctx, "")
		<-ctx.Done()
		ev.Listenner2("events").Unsubscribe2("scalar")
		ev.Close()
	})
	Convey("When do panic in handler should not panic", t, func() {
		ev, err := Dial(mongoURL, dbName)
		So(err, ShouldBeNil)
		So(ev, ShouldNotBeNil)
		err = ev.Listenner2("events").Subscribe2("scalar", panicHandler)
		So(err, ShouldBeNil)
		ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
		go ev.Listenner2("events").Listen(ctx, "")
		<-ctx.Done()
		ev.Listenner2("events").Unsubscribe2("scalar")
		ev.Close()
	})
	Convey("Check if LastId isn't empty string", t, func() {
		dropTestDatabase(dbName)
		ev, err := Dial(mongoURL, dbName)
		So(err, ShouldBeNil)
		So(ev, ShouldNotBeNil)
		ev.Committer("events").SubmitEvent("", "fake", "{\"event\":\"fake\"}")
		So(err, ShouldBeNil)
		id := ev.Listenner2("events").GetLastID()
		So(id, ShouldNotEqual, "")
		ev.Close()

	})
}
func TestQueryInterface(t *testing.T) {
	Convey("When publish 100 messages", t, func() {
		var m map[string]interface{}
		evStore, err := Dial(mongoURL, dbName)
		So(err, ShouldBeNil)
		evStore.Manager().DropDatabase(dbName)
		for i := 0; i < 100; i++ {
			expected := "{\"message\":" + strconv.Itoa(i) + "}"
			evStore.Committer("test").SubmitEvent("", "test", expected)
		}
		Convey("They should be published continousely. And returned in opposit order", func() {
			c, err := evStore.Query("test").Find(bson.M{}, "-$natural")
			So(err, ShouldBeNil)
			So(c, ShouldNotBeNil)
			messageCounter := 0
			for msg := range c {
				err = json.Unmarshal([]byte(msg), &m)
				So(err, ShouldBeNil)
				So(m["event"].(map[string]interface{})["message"], ShouldEqual, 99-messageCounter)
				messageCounter = messageCounter + 1
			}
			So(messageCounter, ShouldEqual, 100)
		})
	})
	Convey("When requesting pipeline", t, func() {
		var m map[string]interface{}
		evStore, err := Dial(mongoURL, dbName)
		So(err, ShouldBeNil)
		evStore.Manager().DropDatabase(dbName)
		notExpected := "{\"message\":\"NOT expected\"}"
		expected := "{\"message\":\"expected\"}"
		evStore.Committer("test").SubmitEvent("", "test", notExpected)
		<-time.After(200 * time.Millisecond)
		tBegin := time.Now()
		evStore.Committer("test").SubmitEvent("", "test", expected)
		<-time.After(200 * time.Millisecond)
		tEnd := time.Now()
		fakePipeline := make([]bson.M, 1)
		fakePipeline[0] = bson.M{"$match": bson.M{"timestamp": bson.M{"$gte": tBegin, "$lt": tEnd}}}
		c, err := evStore.Query("test").Pipe(fakePipeline)
		So(err, ShouldBeNil)
		So(c, ShouldNotBeNil)
		msg, ok := <-c
		if !ok {
			t.Fatal("No message returned. Channel closed")
		}
		err = json.Unmarshal([]byte(msg), &m)
		So(err, ShouldBeNil)
		So(m["event"].(map[string]interface{})["message"], ShouldEqual, "expected")

	})
	Convey("When post 100 messages", t, func() {
		var m map[string]interface{}
		evStore, err := Dial(mongoURL, dbName)
		So(err, ShouldBeNil)
		evStore.Manager().DropDatabase(dbName)
		for i := 0; i < 100; i++ {
			expected := "{\"message\":" + strconv.Itoa(i) + "}"
			evStore.Committer("test").SubmitEvent("", "test", expected)
		}
		Convey("First Event should have value 0", func() {
			c, err := evStore.Query("test").FindOne(bson.M{}, "$natural")
			So(err, ShouldBeNil)
			So(c, ShouldNotBeNil)
			messageCounter := 0
			for msg := range c {
				err = json.Unmarshal([]byte(msg), &m)
				So(err, ShouldBeNil)
				So(m["event"].(map[string]interface{})["message"], ShouldEqual, 0)
				messageCounter = messageCounter + 1
			}
			Convey("The only one message should be returned", func() {
				So(messageCounter, ShouldEqual, 1)
			})
		})
	})
}
func TestManagerInterface(t *testing.T) {
	Convey("Checks if it is ok to drop missed database", t, func() {
		ev, err := Dial(mongoURL, dbName)
		So(err, ShouldBeNil)
		So(ev, ShouldNotBeNil)
		err = ev.Manager().DropDatabase("NONEXISTINGDATABASE")
		So(err, ShouldBeNil)
	})
	Convey("Checks if existing database is realy dropped", t, func() {
		ev, err := Dial(mongoURL, dbName)
		So(err, ShouldBeNil)
		So(ev, ShouldNotBeNil)
		Convey("Submit some data to "+dbName+" database", func() {
			err = ev.Committer("events").PrepareStream()
			So(err, ShouldBeNil)
			err = ev.Committer("events").SubmitEvent("", "test", "{\"message\":\"not empty database\"}")
			So(err, ShouldBeNil)
		})
		Convey("Checks if test database exists", func() {
			names, err1 := getDatabaseNames()
			So(err1, ShouldBeNil)
			bExists := false
			log.Println(names)
			for n := range names {
				log.Println(names[n])
				if names[n] == dbName {
					bExists = true
				}
			}
			So(bExists, ShouldBeTrue)
		})
		Convey("Checks if database is realy dropped", func() {
			err = ev.Manager().DropDatabase(dbName)
			So(err, ShouldBeNil)
			names, err := getDatabaseNames()
			So(err, ShouldBeNil)
			for n := range names {
				log.Println(names[n])
				So(names[n], ShouldNotEqual, dbName)
			}
		})
	})
	Convey("Check if list of existing databases not empty", t, func() {
		ev, err := Dial(mongoURL, dbName)
		So(err, ShouldBeNil)
		dbList, err := ev.Manager().DatabaseNames()
		So(err, ShouldBeNil)
		So(len(dbList), ShouldBeGreaterThan, 0)
	})
	Convey("Check if test database has collections", t, func() {
		ev, err := Dial(mongoURL, dbName)
		So(err, ShouldBeNil)
		err = ev.Committer("events").PrepareStream()
		So(err, ShouldBeNil)
		err = ev.Committer("events").SubmitEvent("", "test", "{\"message\":\"not empty database\"}")
		So(err, ShouldBeNil)
		collections, err := ev.Manager().CollectionNames()
		So(err, ShouldBeNil)
		So(len(collections), ShouldBeGreaterThan, 0)
	})

}
func sampleHandler(ctx context.Context, events []interface{}) {
	for _, event := range events {
		log.Println(event)
	}
}
func panicHandler(ctx context.Context, event []interface{}) {
	panic("panic Handler fired")
}
