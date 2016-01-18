package current

import "testing"

func TestStateReader(t *testing.T) {
	stateReader := NewStateReader()
	if stateReader == nil {
		t.Error("stateReader is nil")
	}
}
func TestSubscribeStateChanges(t *testing.T) {
	stateReader := NewStateReader()
	ch, err := stateReader.SubscribeStateChanges("")
	if err != nil {
		t.Error("SubscribeStateChanges error. ", err)
	}
	if ch == nil {
		t.Error("Channel returned is invalid")
	}
}
func TestStateWriter(t *testing.T) {
	stateWriter := NewStateWriter()
	if stateWriter == nil {
		t.Error("stateWriter is nil")
	}
}

func TestUpdateState(t *testing.T) {
	stateWriter := NewStateWriter()
	err := stateWriter.UpdateState("", "")
	if err != nil {
		t.Error("UpdateState error. ", err)
	}
}
