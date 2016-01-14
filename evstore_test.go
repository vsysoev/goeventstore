package evstore

import (
	"encoding/json"
	"testing"

	"gopkg.in/mgo.v2"

	. "github.com/smartystreets/goconvey/convey"
)

const mongoURL string = "mongodb://127.0.0.1"

type (
	PositiveEventReader struct{}
)

func NewPositiveEventReader() EventReader {
	return &PositiveEventReader{}
}

func (e *PositiveEventReader) Dial(url string, dbName string, eventCollection string) error {
	return nil
}

func (e *PositiveEventReader) ReadEvents(fromID string) (chan string, error) {
	return nil, nil
}

func (e *PositiveEventReader) Subscribe(fromID string) (chan string, error) {
	return nil, nil
}

func (e *PositiveEventReader) Unsubscribe(eventChannel chan string) {

}

func (e *PositiveEventReader) Close() {
	return
}

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
	Convey("1 should equal 1", t, func() {
		So(1, ShouldEqual, 1)
	})
	Convey("Test if contains works", t, func() {
		arr := []string{"value1", "value2", "value3"}
		So(contains(arr, "value1"), ShouldBeTrue)
		So(contains(arr, "value3"), ShouldBeTrue)
		So(contains(arr, "value5"), ShouldBeFalse)
	})
	Convey("Test MongoEventReader", t, func() {
		mng := NewMongoEventReader()
		So(mng, ShouldNotBeNil)
		err := mng.Dial(mongoURL, "test", "events")
		So(err, ShouldBeNil)
	})
	Convey("Test CommitEvent of MongoEventWriter", t, func() {
		mng := NewMongoEventWriter()
		So(mng, ShouldNotBeNil)
		err := mng.Dial(mongoURL, "test", "events")
		So(err, ShouldBeNil)
		err = mng.CommitEvent("{\"test\":\"value\"}")
		So(err, ShouldBeNil)
		err = mng.CommitEvent("{\"array\":[\"123\",\"345\", 45, 3445.456]}")
		So(err, ShouldBeNil)
	})
	Convey("Test EventReader", t, func() {
		ev := NewPositiveEventReader()
		So(ev, ShouldNotBeNil)
		err := ev.Dial("fake", "test", "events")
		So(err, ShouldBeNil)
		out, err := ev.Subscribe("")
		So(err, ShouldBeNil)
		So(out, ShouldBeNil)
		ev.Unsubscribe(out)
		ev.Close()
	})
	Convey("ReadEvents from the database", t, func() {
		mng := NewMongoEventReader()
		So(mng, ShouldNotBeNil)
		err := mng.Dial(mongoURL, "test", "events")
		So(err, ShouldBeNil)
		ch, err := mng.Subscribe("")
		So(err, ShouldBeNil)
		var lastID string
		for s := range ch {
			So(s, ShouldNotEqual, "")
			var js map[string]interface{}
			e := json.Unmarshal([]byte(s), &js)
			So(e, ShouldBeNil)
			lastID = js["_id"].(string)
			So(lastID, ShouldNotEqual, "")
			mng.Unsubscribe(ch)
		}
	})
	Convey("Close connection", t, func() {
		mng := NewMongoEventReader()
		So(mng, ShouldNotBeNil)
		err := mng.Dial(mongoURL, "test", "events")
		So(err, ShouldBeNil)
		ch, err := mng.Subscribe("")
		So(err, ShouldBeNil)
		So(ch, ShouldNotBeNil)
		mng.Unsubscribe(ch)
		mng.Close()
	})

}
