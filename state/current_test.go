package current

import "testing"

const mongoURL string = "mongodb://127.0.0.1"

func TestValueTIncorrect(t *testing.T) {
	v := ValueT{}
	if err := v.Set("", ""); err == nil {
		t.Error("err should not be nil")
	}

}
func TestValueTPositive(t *testing.T) {
	v := ValueT{}
	err := v.Set("This is id", 123)
	if err != nil {
		t.Error("Error settering 123 value", err)
	}
	k, val := v.Get()
	if k != "This is id" && val != 123 {
		t.Error("Error getting key integer value.", k, val, v)
	}
	err = v.Set("float", 132.234)
	if err != nil {
		t.Error("Error setting float value.", err)
	}
	k, val = v.Get()
	if k != "float" && val != 123.234 {
		t.Error("Error getting key, value", k, val)
	}
	if err = v.Set("string", "value"); err != nil {
		t.Error("Error setting string value", err)
	}
	k, val = v.Get()
	if k != "string" && val != "value" {
		t.Error("Error getting string value.", k, val)
	}
}

func TestState(t *testing.T) {
	stateReader, stateUpdater := NewState()
	if stateReader == nil {
		t.Error("stateReader is nil")
	}
	if stateUpdater == nil {
		t.Error("stateUpdater is nil")
	}
}

func TestGetter(t *testing.T) {
	stateReader, _ := NewState()
	if stateReader == nil {
		t.Error("stateReader creation error")
	}
	_, err := stateReader.Get("")
	if err != nil {
		t.Error("Getter error.", err)
	}
}
func TestStateReadUpdate(t *testing.T) {
	stateReader, stateUpdater := NewState()
	if stateUpdater == nil {
		t.Fatal("StateUpdater creation error")
	}
	if stateReader == nil {
		t.Fatal("StateReader creation error")
	}

	err := stateUpdater.Update("k1", "1", 23)
	if err != nil {
		t.Fatal("Error in UpdateState.", err)
	}
	valueMap, err := stateReader.Get("")
	if valueMap == nil || err != nil {
		t.Error("Error getting state.", err)
	}
	if valueMap["k1"] != 23 {
		t.Error("Error getting value.", valueMap["k1"])
	}
}
