package evstore

import "testing"

func TestTSEmptyPoint(t *testing.T) {
	dropTestDatabase(dbName)
	ev, err := Dial(mongoURL, dbName)
	if err != nil {
		t.Fatal(err)
	}
	if ev == nil {
		t.Fatal("ev should not be nil")
	}
	point := ""
	err = ev.Timeseries("test").Submit(point)
	if err != nil {
		if err.Error() != "unexpected end of JSON input" {
			t.Fatal(err)
		}
	}
	point = "{}"
	err = ev.Timeseries("test").Submit(point)
	if err != nil {
		if err.Error() != "unexpected end of JSON input" {
			t.Fatal(err)
		}
	}
	ev.Close()
}
func TestTSOneObject(t *testing.T) {
	dropTestDatabase(dbName)
	ev, err := Dial(mongoURL, dbName)
	if err != nil {
		t.Fatal(err)
	}
	if ev == nil {
		t.Fatal("ev should not be nil")
	}
	point := "{\"tag\":\"scalar\"}"
	err = ev.Timeseries("test").Submit(point)
	if err != nil {
		t.Fatal(err)
	}
	params := map[string]interface{}{}
	ch, err := ev.Timeseries("test").Query(params)
	if ch == nil {
		t.Fatal("channel shouldn't be nil")
	}
	if err != nil {
		t.Fatal(err)
	}
	ev.Close()
}
