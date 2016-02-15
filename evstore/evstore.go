package evstore

import (
	"encoding/json"
	"errors"
	"log"
	"sync"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	chanBufSize int = 1024
)

type (
	// Connection exports mondodb connection attributes
	Connection struct {
		session       *mgo.Session
		dbName        string
		stream        string
		triggerStream string
		committer     *CommitterT
		listenner     *ListennerT
	}
	// CommitterT exports Committer interface
	CommitterT struct {
		p *Connection
	}
	// ListennerT export Listenner interface
	ListennerT struct {
		p      *Connection
		filter map[string]Handler
		stream string
		done   chan bool
		wg     *sync.WaitGroup
	}
	// Committer interface defines method to commit new event to eventstore
	Committer interface {
		SubmitEvent(sequenceID string, tag string, eventJSON string) error
	}
	// Listenner interface defines method to listen events from the datastore
	Listenner interface {
		Subscribe(fromID string) (chan string, error)
		Unsubscribe(eventChannel chan string)
	}

	// Handler type defines function which will be used as callback
	Handler    func(event interface{})
	Listenner2 interface {
		Subscribe2(eventTypes string, handlerFunc Handler) error
		Unsubscribe2(eventTypes string)
		Listen(id string, done <-chan bool) error
	}
)

// Dial fabric function to produce new connection to eventstore
// url - path to the mongodb server for ex. mongodb://127.0.0.1
// dbName - database name where events are stored
// stream - collection name where events are stored. EventSourcing stream
func Dial(url string, dbName string, stream string) (*Connection, error) {
	var err error
	c := Connection{}
	c.session, err = mgo.Dial(url)
	if err != nil {
		return nil, err
	}
	c.dbName = dbName
	c.stream = stream
	c.triggerStream = stream + "_capped"
	c.committer = &CommitterT{}
	c.listenner = &ListennerT{}
	c.listenner.p = &c
	c.committer.p = &c
	c.listenner.wg = &sync.WaitGroup{}
	collections, err := c.session.DB(dbName).CollectionNames()
	if err != nil {
		return nil, err
	}
	if !contains(collections, c.triggerStream) {
		cInfo := mgo.CollectionInfo{
			Capped:   true,
			MaxBytes: 1000000,
		}
		c.session.DB(dbName).C(c.triggerStream).Create(&cInfo)
	}
	return &c, nil
}

// Close closes connection to event store
func (c *Connection) Close() {
	c.session.Close()
}

// Committer return committer object which implement Committer interface
func (c *Connection) Committer() Committer {
	return c.committer
}

// Listenner returns listenner object which implments Listenner interface
func (c *Connection) Listenner() Listenner {
	return c.listenner
}

// SubmitEvent submittes event to event store
func (c *CommitterT) SubmitEvent(sequenceID string, tag string, eventJSON string) error {
	var object map[string]interface{}
	err := json.Unmarshal([]byte(eventJSON), &object)
	if err != nil {
		log.Println(err)
		return err
	}
	event := make(map[string]interface{})
	event["sequenceID"] = sequenceID
	event["tag"] = tag
	event["event"] = object
	err = c.p.session.DB(c.p.dbName).C(c.p.stream).Insert(event)
	if err != nil {
		log.Println(err)
		return err
	}
	err = c.p.session.DB(c.p.dbName).C(c.p.triggerStream).Insert(bson.M{"trigger": 1})
	if err != nil {
		log.Println(err)
	}
	return err
}

// ReadEvents Read JSON events started from fromId to slice of strings
func (e *ListennerT) readEvents(fromID string) (*mgo.Iter, error) {
	var (
		iter *mgo.Iter
	)
	if fromID != "" {
		if !bson.IsObjectIdHex(fromID) {
			return nil, errors.New("Error: Incorrect fromID " + fromID)
		}
		objID := bson.ObjectIdHex(fromID)
		iter = e.p.session.DB(e.p.dbName).C(e.p.stream).Find(bson.M{"_id": bson.M{"$gt": objID}}).Iter()
	} else {
		iter = e.p.session.DB(e.p.dbName).C(e.p.stream).Find(nil).Iter()
	}
	return iter, nil
}

// Subscribe returns channel from event store
func (e *ListennerT) Subscribe(fromID string) (chan string, error) {
	if e.p.session == nil {
		return nil, errors.New("Mongo isn't connected. Please use Dial().")
	}
	cTrigger := e.p.session.DB(e.p.dbName).C(e.p.triggerStream)
	var lastTriggerID string
	var lastEventID string
	if fromID != "" {
		lastEventID = fromID
	}
	var result map[string]interface{}
	outChan := make(chan string, chanBufSize)
	e.done = make(chan bool)
	go func() {
		var (
			iter     *mgo.Iter
			oneEvent map[string]interface{}
		)
		defer func() {
			e.done <- true
			close(outChan)
		}()
		iterLast := cTrigger.Find(nil).Sort("-$natural").Limit(2).Iter()
		for iterLast.Next(&result) {
			lastTriggerID = result["_id"].(bson.ObjectId).Hex()
		}
		iterLast.Close()
		iter = cTrigger.Find(bson.M{"_id": bson.M{"$gt": bson.ObjectIdHex(lastTriggerID)}}).Sort("$natural").Tail(10 * time.Millisecond)
	Loop:
		for {
			for iter.Next(&result) {
				lastTriggerID = result["_id"].(bson.ObjectId).Hex()
				evIter, err := e.readEvents(lastEventID)
				if err != nil {
					log.Println(err)
					return
				}
				for evIter.Next(&oneEvent) {
					lastEventID = string(oneEvent["_id"].(bson.ObjectId).Hex())
					s, err := json.Marshal(oneEvent)
					if err != nil {
						log.Println(err)
						return
					}
					select {
					case <-e.done:
						break Loop
					default:
						break
					}
					outChan <- string(s)
				}
				evIter.Close()
			}
			er := iter.Err()
			if er != nil {
				log.Println(er)
				return
			}
			select {
			case <-e.done:
				break Loop
			default:
				break
			}
			if iter.Timeout() {
				continue
			}
			qNext := cTrigger.Find(bson.M{"_id": bson.M{"$gt": lastTriggerID}})

			iter = qNext.Sort("$natural").Tail(10 * time.Millisecond)
		}
		return
	}()
	return outChan, nil
}

// Unsubscribe closes channel
func (e *ListennerT) Unsubscribe(eventChannel chan string) {
	if eventChannel == nil {
		return
	}
	select {
	case <-eventChannel:
		break
	default:
		break
	}
	e.done <- true
}

func (c *Connection) Listenner2() Listenner2 {
	return c.listenner
}

func (e *ListennerT) Subscribe2(eventType string, handlerFunc Handler) error {

	if eventType != "" {
		if e.filter == nil {
			e.filter = make(map[string]Handler)
		}
		e.filter[eventType] = handlerFunc
	} else {
		e.filter = nil
		e.filter = make(map[string]Handler)
		e.filter[""] = handlerFunc
	}
	return nil
}

func (e *ListennerT) Unsubscribe2(eventType string) {
	return
}

// Listen start go routines which listen event in event stream and execute Handler
// TODO: Handler should be executed with panic/recover
func (e *ListennerT) Listen(id string, done <-chan bool) error {
	if e.p.session == nil {
		return errors.New("Mongo isn't connected. Please use Dial().")
	}
	for filter, handler := range e.filter {
		e.wg.Add(1)
		go e.processSubscription(filter, id, handler, e.wg, done)
	}
	e.wg.Wait()
	return nil
}
func (e *ListennerT) readEventsLimit(filter string, fromID string, limit int) ([]interface{}, error) {
	var (
		err    error
		result []interface{}
		q      bson.M
	)
	q = make(bson.M)
	f := []string{filter}
	if filter != "" {
		q["tag"] = bson.M{ "$in": f}
	}
	if fromID != "" {
		if !bson.IsObjectIdHex(fromID) {
			return nil, errors.New("Error: Incorrect fromID " + fromID)
		}
		objID := bson.ObjectIdHex(fromID)
		q["_id"] = bson.M{"$gt": objID}
	}
	err = e.p.session.DB(e.p.dbName).C(e.p.stream).Find(q).Limit(limit).All(&result)
	return result, err
}

func (e *ListennerT) processSubscription(filter string, id string, handler Handler, wg *sync.WaitGroup, done <-chan bool) {
	defer func() {
		// TODO: Here should be panic/recover
		wg.Done()
	}()
	for {
		result, err := e.readEventsLimit(filter, id, 100)
		if err != nil {
			log.Println(err)
			return
		}
		for _, ev := range result {
			handler(ev)
			id = ev.(bson.M)["_id"].(bson.ObjectId).Hex()
			log.Println("Last objectID", id)
		}
		select {
		case <-done:
			return
		case <-time.After(time.Millisecond *10):
			break
		}

	}
	return
}

func contains(col []string, target string) bool {
	for _, s := range col {
		if s == target {
			return true
		}
	}
	return false
}
