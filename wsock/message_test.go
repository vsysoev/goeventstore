package wsock

import (
	"encoding/json"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestDropDatabase(t *testing.T) {
	Convey("Messag should be stringigied", t, func() {
		var m MessageT
		msgExpected := "{\"value\":\"test\"}"

		err := json.Unmarshal([]byte(msgExpected), &m)
		So(err, ShouldBeNil)
		msg := m.String()
		So(err, ShouldBeNil)
		So(msgExpected, ShouldEqual, string(msg))
	})
}
