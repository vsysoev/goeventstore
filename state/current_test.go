package current

import "testing"

const mongoURL string = "mongodb://127.0.0.1"

func TestStateReader(t *testing.T) {
	stateReader := NewStateReader()
	if stateReader == nil {
		t.Error("stateReader is nil")
	}
}
func TestSubscribeStateChanges(t *testing.T) {
	stateReader := NewStateReader()
	if stateReader == nil {
		t.Error("stateReader creation error")
	}
	ch, err := stateReader.SubscribeStateChanges("")
	if err != nil {
		t.Error("SubscribeStateChanges error. ", err)
	}
	if ch == nil {
		t.Error("Channel returned is invalid")
	}
	stateReader.UnsubscribeStateChanges(ch)
}
func TestStateWriter(t *testing.T) {
	stateWriter := NewStateWriter()
	stateReader := NewStateReader()
	if stateWriter == nil {
		t.Fatal("StateWriter creation error")
	}
	if stateReader == nil {
		t.Fatal("StateReader creation error")
	}
	ch, err := stateReader.SubscribeStateChanges("")
	if ch == nil || err != nil {
		t.Fatal("Error read channel")
	}

	err = stateWriter.UpdateState("1", 23)
	if err != nil {
		t.Fatal("Error in UpdateState")
	}
	s := <-ch
	if s == "" {
		t.Error("Incorrect read of state update")
	}
}
