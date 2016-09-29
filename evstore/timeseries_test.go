package evstore

import "testing"

func TestTimeSeries(t *testing.T) {
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
		t.Fatal(err)
	}
	ev.Close()

}
