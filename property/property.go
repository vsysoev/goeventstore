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
	props["current.uri"] = "/current"
	props["current.url"] = ":8899"
	props["events.uri"] = "/events"
	props["events.url"] = ":8890"
	props["static.url"] = "/"
	props["submitevents.uri"] = "/events"
	props["submitevents.url"] = ":8891"
	props["postevents.uri"] = "/postevent"
	props["securepostevents.url"] = ":8892"
	props["configsrv.uri"] = "/config"
	props["configsrv.url"] = ":8893"
	props["configsrv.stream"] = "config"
	props["rpceventsrv.uri"] = "/rpc"
	props["rpceventsrv.url"] = ":8894"
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
