package property

import "encoding/json"

// PropSet holds maps of properties
type PropSet map[string]string

// Init inits default values
func Init() PropSet {
	props := make(map[string]string)
	props["mongodb.url"] = "mongodb://127.0.0.1"
	props["mongodb.port"] = "27017"
	props["mongodb.db"] = "mt"
	props["mongodb.stream"] = "events"
	props["websocket.uri"] = "/ws"
	props["websocket.url"] = ":8899"
	props["static.url"] = "/"
	return props
}

// LoadFromJSON updates default PropSet from Init function with
// values from JSON. If value doesn't exists it creates.
func (p PropSet) LoadFromJSON(js []byte) error {
	var params map[string]interface{}
	err := json.Unmarshal(js, &params)
	if err != nil {
		return err
	}
	for k, v := range params {
		p[k] = v.(string)
	}
	return nil
}
