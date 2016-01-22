package property

// Init inits default values
func Init() map[string]interface{} {
	props := make(map[string]interface{})
	props["mongodb.url"] = "mongodb://127.0.0.1"
	props["mongodb.port"] = 27017
	props["mongodb.db"] = "eventstore"
	props["mongodb.events"] = "events"
	props["websocket.uri"] = "/ws"
	props["websocket.url"] = ":8899"
	return props
}
