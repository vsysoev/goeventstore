package current

import "errors"

type (
	// StateReader defines interface which is used for Subscribtion to state changes
	StateReader interface {
		Get(id string) (map[string]interface{}, error)
	}

	// StateUpdater defines interface which updates state
	StateUpdater interface {
		Update(key string, id string, value interface{}) error
	}
	// ValueT type describes one value in store
	ValueT struct {
		id    string
		value interface{}
	}
	// State holds current state of the values of the system
	State struct {
		// Store holds key-value
		Store map[string]*ValueT
	}
)

// Set is the setter for the ValueT type
func (v *ValueT) Set(id string, value interface{}) error {
	if id == "" {
		return errors.New("id should not be empty string")
	}
	v.id = id
	v.value = value
	return nil
}

// Get is the getter for the ValueT type
func (v *ValueT) Get() (string, interface{}) {
	return v.id, v.value
}

// NewStateReader contructor of the state reader
func NewStateReader() StateReader {
	s := State{}
	return &s
}

// NewState returns pointer to read and update interface
func NewState() (StateReader, StateUpdater) {
	s := State{}
	return &s, &s
}

// Get returns map[string]inteface{} of values which has been changed since id
func (s *State) Get(id string) (map[string]interface{}, error) {
	ret := make(map[string]interface{})
	for k, v := range s.Store {
		_id, _v := v.Get()
		if _id > id {
			ret[k] = _v
		}
	}
	return ret, nil
}

// NewStateUpdater create object with StateUpdater interface
func NewStateUpdater() StateUpdater {
	return &State{}
}

// Update update value with id with new value
func (s *State) Update(key string, id string, value interface{}) error {
	if key == "" {
		return errors.New("key should not be empty string")
	}
	if s.Store == nil {
		s.Store = make(map[string]*ValueT)
	}
	if s.Store[key] == nil {
		s.Store[key] = &ValueT{}
	}
	return s.Store[key].Set(id, value)
}
