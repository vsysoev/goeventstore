package main

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestCurrentState(t *testing.T) {
	Convey("1 should equal 1", t, func() {
		So(1, ShouldEqual, 1)
	})
	Convey("sState initialized", t, func() {
		So(sysState, ShouldNotBeNil)
		So(sysState.scalar.Value, ShouldBeZeroValue)
		So(sysState.scalar.BoxID, ShouldBeZeroValue)
		So(sysState.scalar.VarID, ShouldBeZeroValue)
		So(sysState.scalar.TimePoint, ShouldBeZeroValue)
	})
	Convey("Test eventReader", t, func() {
		ev, err := eventReader("test")
		So(ev, ShouldNotBeNil)
		So(err, ShouldBeNil)
	})
	Convey("Run loadSystemState", t, func() {
		ch := make(chan string)
		loadSystemState(ch)
		So(sysState, ShouldNotBeNil)
		So(sysState.scalar.Value, ShouldEqual, 1.0)
		So(sysState.scalar.BoxID, ShouldEqual, 1)
		So(sysState.scalar.VarID, ShouldEqual, 1)
		So(sysState.scalar.TimePoint, ShouldEqual, 1)
	})
	Convey("Run main", t, func() {
		main()
		So(sysState, ShouldNotBeNil)
		So(sysState.scalar.Value, ShouldEqual, 1.0)
		So(sysState.scalar.BoxID, ShouldEqual, 1)
		So(sysState.scalar.VarID, ShouldEqual, 1)
		So(sysState.scalar.TimePoint, ShouldEqual, 1)
	})
}
