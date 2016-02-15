package eventtypes

import "time"

type (
	ScalarRecieved struct {
		MessageID string
		DateStamp time.Time
		BoxID     int
		VarID     int
		Value     float32
	}
)
