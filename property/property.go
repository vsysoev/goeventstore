package property

import "encoding/json"

type PropSet map[string]string

// Init inits default values
func Init() PropSet {
	props := make(map[string]string)
	props["mongodb.url"] = "mongodb://127.0.0.1"
	props["mongodb.port"] = "27017"
	props["mongodb.db"] = "eventstore"
	props["mongodb.events"] = "events"
	props["websocket.uri"] = "/ws"
	props["websocket.url"] = ":8899"
	return props
}

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
