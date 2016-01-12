package evstore

import (
	"encoding/json"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

type (
	PositiveEventReader struct{}
)

func NewPositiveEventReader() EventReader {
	return &PositiveEventReader{}
}

func (e *PositiveEventReader) Dial(url string) error {
	return nil
}

func (e *PositiveEventReader) SetEventStore(dbName string, colName string) {

}
func (e *PositiveEventReader) ReadEvents(fromId int) (chan string, error) {
	return nil, nil
}

func (e *PositiveEventReader) Subscribe(fromId int) (chan string, error) {
	return nil, nil
}
func (e *PositiveEventReader) Close() {
	return
}

func TestJSONUnmarshalling(t *testing.T) {
	Convey("Simple json unmarshalling", t, func() {
		simpleJson := "{\"name\":\"value\"}"
		var objmap interface{}
		err := json.Unmarshal([]byte(simpleJson), &objmap)
		So(err, ShouldBeNil)
	})
}

func TestCurrentState(t *testing.T) {
	Convey("1 should equal 1", t, func() {
		So(1, ShouldEqual, 1)
	})
	Convey("Test MongoEventReader", t, func() {
		mng := NewMongoEventReader()
		So(mng, ShouldNotBeNil)
		err := mng.Dial("mongodb://127.0.0.1/test")
		So(err, ShouldBeNil)
	})
	Convey("Test CommitEvent of MongoEventWriter", t, func() {
		mng := NewMongoEventWriter()
		So(mng, ShouldNotBeNil)
		err := mng.Dial("mongodb://127.0.0.1/test")
		So(err, ShouldBeNil)
		mng.SetEventStore("test", "events", "eventid")
		err = mng.CommitEvent("{\"test\":\"value\"}")
		So(err, ShouldBeNil)
		err = mng.CommitEvent("{\"array\":[\"123\",\"345\", 45, 3445.456]}")
		So(err, ShouldBeNil)
	})
	Convey("Test EventReader", t, func() {
		ev := NewPositiveEventReader()
		So(ev, ShouldNotBeNil)
		err := ev.Dial("test")
		So(err, ShouldBeNil)
		out, err := ev.Subscribe(-10)
		So(err, ShouldBeNil)
		So(out, ShouldBeNil)
		ev.Close()
	})
	Convey("ReadEvents from the database", t, func() {
		mng := NewMongoEventReader()
		So(mng, ShouldNotBeNil)
		err := mng.Dial("mongodb://127.0.0.1/test")
		So(err, ShouldBeNil)
		mng.SetEventStore("test", "events")
		ch, err := mng.Subscribe(-1)
		So(err, ShouldBeNil)
		var lastID int
		prevID := lastID
		for s := range ch {
			So(s, ShouldNotEqual, "")
			var js map[string]interface{}
			e := json.Unmarshal([]byte(s), &js)
			So(e, ShouldBeNil)
			prevID = lastID
			lastID = int(js["eventid"].(float64))
		}
		ch, err = mng.Subscribe(prevID)
		So(err, ShouldBeNil)
		itemCount := 0
		for s := range ch {
			itemCount++
			var js map[string]interface{}
			e := json.Unmarshal([]byte(s), &js)
			So(e, ShouldBeNil)
			ev := js["event"].(map[string]interface{})
			a := ev["array"].([]interface{})
			So(a[0].(string), ShouldEqual, "123")
			So(a[1].(string), ShouldEqual, "345")
			So(a[2].(float64), ShouldEqual, 45)
			So(a[3].(float64), ShouldEqual, 3445.456)
		}
		So(itemCount, ShouldEqual, 1)
	})
	/*
		Convey("Run main", t, func() {
			main()
			So(sysState, ShouldNotBeNil)
			So(sysState.scalar.Value, ShouldEqual, 1.0)
			So(sysState.scalar.BoxID, ShouldEqual, 1)
			So(sysState.scalar.VarID, ShouldEqual, 1)
			So(sysState.scalar.TimePoint, ShouldEqual, 1)
		})
	*/
}
