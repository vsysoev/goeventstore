package current

import "errors"

type (
	// StateReader defines interface which is used for Subscribtion to state changes
	StateReader interface {
		SubscribeStateChanges(id string) (chan string, error)
	}
	// StateWriter defines interface which is used to update currentstate.
	StateWriter interface {
		UpdateState(id string, value string) error
	}
	// State holds current state of the values of the system
	State struct {
		Store map[string]interface{}
	}
)

// NewStateReader contructor of the state reader
func NewStateReader() StateReader {
	return &State{}
}

func (s *State) SubscribeStateChanges(id string) (chan string, error) {
	return nil, errors.New("Not implemented")
}

func NewStateWriter() StateWriter {
	return &State{}
}

func (s *State) UpdateState(id string, value string) error {
	return errors.New("Not Implemented")
}
