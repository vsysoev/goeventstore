package current

import "errors"

type (
	// StateReader defines interface which is used for Subscribtion to state changes
	StateReader interface {
		SubscribeStateChanges(id string) (chan string, error)
		UnsubscribeStateChanges(ch chan string) error
	}

	// StateWriter defines interface which updates state
	StateWriter interface {
		UpdateState(id string, value interface{}) error
	}
	// State holds current state of the values of the system
	State struct {
		Store map[string]interface{}
	}
)

// NewStateReader contructor of the state reader
func NewStateReader() StateReader {
	s := State{}
	return &s
}

func (s *State) SubscribeStateChanges(id string) (chan string, error) {

	return nil, errors.New("Not implemented")
}

func (s *State) UnsubscribeStateChanges(ch chan string) error {

	return errors.New("Not implemented")
}

func NewStateWriter() StateWriter {
	return &State{}
}
func (s *State) UpdateState(id string, value interface{}) error {
	return errors.New("Not implemented")
}
