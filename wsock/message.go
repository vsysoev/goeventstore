package wsock

import "encoding/json"

type (
	MessageT map[string]interface{}
)

func (s MessageT) String() string {
	msg, err := json.Marshal(s)
	if err != nil {
		return ""
	}
	return string(msg)
}
