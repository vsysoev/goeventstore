package main

import (
	"testing"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/vsysoev/goeventstore/evstore"
	"github.com/vsysoev/goeventstore/property"
)

func TestDropDatabase(t *testing.T) {
	Convey("Before start testing we drop database", t, func() {
		props := property.Init()
		session, err := mgo.Dial(props["mongodb.url"])
		So(err, ShouldBeNil)
		err = session.DB("test").DropDatabase()
		So(err, ShouldBeNil)

	})
}

func TestSubmitEvent(t *testing.T) {
	Convey("Submit simple event", t, func() {
		var results []interface{}
		props := property.Init()
		ev, err := evstore.Dial(props["mongodb.url"], "test", "events")
		So(err, ShouldBeNil)
		So(ev, ShouldNotBeNil)
		err = ev.Committer().SubmitEvent("", "test", "{\"event\":\"fake\"}")
		So(err, ShouldBeNil)
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
