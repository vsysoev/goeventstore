package property

import "testing"

func TestInitProp(t *testing.T) {
	props := Init()
	if props["mongodb.url"] != "mongodb://127.0.0.1" {
		t.Error("Error property mongodb.url", props["mongodb.url"])
	}
	if props["mongodb.port"] != "27017" {
		t.Error("Error property mongodb.port", props["mongodb.port"])
	}
}

func TestJSON(t *testing.T) {
	props := Init()
	if props["mongodb.url"] != "mongodb://127.0.0.1" {
		t.Error("Error property mongodb.url", props["mongodb.url"])
	}
	if props["mongodb.port"] != "27017" {
		t.Error("Error property mongodb.port", props["mongodb.port"])
	}
	err := props.LoadFromJSON([]byte("{\"test\":\"123\"}"))
	if err != nil {
		t.Error("Error parsing JSON.", err)
	}
	if props["test"] != "123" {
		t.Error("Error in parsing JSON test value.", props["test"])
	}
}
