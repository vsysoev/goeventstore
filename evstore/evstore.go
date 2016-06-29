package evstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"golang.org/x/net/context"

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
		l             *ListennerT
		c             *CommitterT
		m             *ManageT
		q             *QueryT
	}
	// CommitterT exports Committer interface
	CommitterT struct {
		c *Connection
	}
	// ListennerT export Listenner interface
	ListennerT struct {
		c      *Connection
		filter map[string]Handler
		stream string
		done   chan bool
		wg     *sync.WaitGroup
	}
	// ManageT struct for Manage interface
	ManageT struct {
		c *Connection
	}

	// QueryT struct for Query interface
	QueryT struct {
		c *Connection
	}

	// Committer interface defines method to commit new event to eventstore
	Committer interface {
		SubmitEvent(sequenceID string, tag string, eventJSON string) error
		SubmitMapStringEvent(sequenceID string, tag string, body map[string]interface{}) error
	}
	// Listenner interface defines method to listen events from the datastore
	Listenner interface {
		Subscribe(fromID string) (chan string, error)
		Unsubscribe(eventChannel chan string)
	}

	// Handler type defines function which will be used as callback
	Handler func(ctx context.Context, event []interface{})
	// Listenner2 interface is replacement of Listenner
	// TODO:10 Remove Listenner interface and rename Listenner2 to Listenner
	Listenner2 interface {
		Subscribe2(eventTypes string, handlerFunc Handler) error
		Unsubscribe2(eventTypes string)
		GetLastID() string
		Listen(ctx context.Context, id string) error
	}
	// Manager interface to support internal database functions
	Manager interface {
		//DropDatabase just drop database
		//TODO:20 Remove after testing will be updated
		DropDatabase(databaseName string) error
	}

	//Query interface to support query from database
	Query interface {
		//Find - return channel with JSON database
		Find(queryParam string) (chan string, error)
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
	c.l = &ListennerT{}
	c.m = &ManageT{}
	c.c = &CommitterT{}
	c.q = &QueryT{}
	c.l.c = &c
	c.m.c = &c
	c.c.c = &c
	c.q.c = &c
	return &c, nil
}

// Close closes connection to event store
func (c *Connection) Close() {
	c.session.Close()
}

// Committer return committer object which implement Committer interface
func (c *Connection) Committer() Committer {
	return c.c
}

// Listenner returns listenner object which implments Listenner interface
func (c *Connection) Listenner() Listenner {
	c.l.wg = &sync.WaitGroup{}
	return c.l
}

// SubmitEvent submittes event to event store
func (c *CommitterT) SubmitEvent(sequenceID string, tag string, eventJSON string) error {
	var object map[string]interface{}
	err := json.Unmarshal([]byte(eventJSON), &object)
	if err != nil {
		log.Println(err)
		return err
	}
	//	nowString := time.Now().Format(time.RFC3339Nano)
	// := map[string]interface{}{"$date": nowString}
	event := make(map[string]interface{})
	event["sequenceID"] = sequenceID
	event["tag"] = tag
	//	event["datestamp"] = datestamp
	event["timestamp"] = time.Now()
	event["event"] = object
	err = c.c.session.DB(c.c.dbName).C(c.c.stream).Insert(event)
	if err != nil {
		log.Println(err)
		return err
	}
	err = c.c.session.DB(c.c.dbName).C(c.c.triggerStream).Insert(bson.M{"trigger": 1})
	if err != nil {
		log.Println(err)
	}
	return err
}

// SubmitMapString submittes event to event store
func (c *CommitterT) SubmitMapStringEvent(sequenceID string, tag string, body map[string]interface{}) error {
	event := make(map[string]interface{})
	event["sequenceID"] = sequenceID
	event["tag"] = tag
	event["event"] = body
	event["timestamp"] = time.Now()
	err := c.c.session.DB(c.c.dbName).C(c.c.stream).Insert(event)
	if err != nil {
		log.Println(err)
		return err
	}
	err = c.c.session.DB(c.c.dbName).C(c.c.triggerStream).Insert(bson.M{"trigger": 1})
	if err != nil {
		log.Println(err)
	}
	return err
}

// ReadEvents Read JSON events started from fromId to slice of strings
func (c *Connection) readEvents(fromID string) (*mgo.Iter, error) {
	var (
		iter *mgo.Iter
	)
	if fromID != "" {
		if !bson.IsObjectIdHex(fromID) {
			return nil, errors.New("Error: Incorrect fromID " + fromID)
		}
		objID := bson.ObjectIdHex(fromID)
		iter = c.session.DB(c.dbName).C(c.stream).Find(bson.M{"_id": bson.M{"$gt": objID}}).Iter()
	} else {
		iter = c.session.DB(c.dbName).C(c.stream).Find(nil).Iter()
	}
	return iter, nil
}

// Subscribe returns channel from event store
func (e *ListennerT) Subscribe(fromID string) (chan string, error) {
	if e.c.session == nil {
		return nil, errors.New("Mongo isn't connected. Please use Dial().")
	}
	cTrigger := e.c.session.DB(e.c.dbName).C(e.c.triggerStream)
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
				evIter, err := e.c.readEvents(lastEventID)
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

// Listenner2 - new implementation of listenner function. Will replace Listenner
func (c *Connection) Listenner2() Listenner2 {
	c.l.wg = &sync.WaitGroup{}
	return c.l
}

// Subscribe2 - new implementation of Subscribe fucntion. Will replace Subscribe
func (e *ListennerT) Subscribe2(eventType string, handlerFunc Handler) error {
	if eventType != "" {
		if len(e.filter) == 0 {
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

// Unsubscribe2 - new implementation of Unsubscribe function.
func (e *ListennerT) Unsubscribe2(eventType string) {
	return
}

// GetLastID - returns last event id
func (e *ListennerT) GetLastID() string {
	var result map[string]interface{}
	iter := e.c.session.DB(e.c.dbName).C(e.c.stream).Find(nil).Sort("-$natural").Limit(1).Iter()
	if iter == nil {
		return ""
	}
	iter.Next(&result)
	if result == nil {
		return ""
	}
	return result["_id"].(bson.ObjectId).Hex()
}

// Listen start go routines which listen event in event stream and execute Handler
func (e *ListennerT) Listen(ctx context.Context, id string) error {
	if e.c.session == nil {
		return errors.New("Mongo isn't connected. Please use Dial().")
	}
	for filter, handler := range e.filter {
		go e.processSubscription(ctx, filter, id, handler)
	}
	<-ctx.Done()
	return nil
}
func (c *Connection) readEventsLimit(filter string, fromID string, limit int) ([]interface{}, error) {
	var (
		err    error
		result []interface{}
		q      bson.M
	)
	sessionCopy := c.session.Copy()
	defer sessionCopy.Close()
	q = make(bson.M)
	f := []string{filter}
	if filter != "" {
		q["tag"] = bson.M{"$in": f}
	}
	if fromID != "" {
		if !bson.IsObjectIdHex(fromID) {
			return nil, errors.New("Error: Incorrect fromID " + fromID)
		}
		objID := bson.ObjectIdHex(fromID)
		q["_id"] = bson.M{"$gt": objID}
	}
	err = sessionCopy.DB(c.dbName).C(c.stream).Find(q).Limit(limit).All(&result)
	return result, err
}

func (e *ListennerT) processSubscription(ctx context.Context, filter string, id string, handler Handler) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Handler function panic detected. Recovering", r)
		}
	}()
	for {
		result, err := e.c.readEventsLimit(filter, id, 1000)
		if err != nil {
			log.Println(err)
			return
		}
		if len(result) > 0 {
			handler(ctx, result)
			id = result[len(result)-1].(bson.M)["_id"].(bson.ObjectId).Hex()
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Millisecond * 10):
			break
		}
	}
}

func contains(col []string, target string) bool {
	for _, s := range col {
		if s == target {
			return true
		}
	}
	return false
}

// Manager - returns Manager interface
func (c *Connection) Manager() Manager {
	return c.m
}

// DropDatabase - drop Mongo Database
func (m *ManageT) DropDatabase(databaseName string) error {
	return m.c.session.DB(databaseName).DropDatabase()
}

// Query request data from database
func (q *QueryT) Query(queryParam string) chan string {
	ch := make(chan string, 256)

	go func() {
		var result []interface{}
		defer close(ch)
		err := q.c.session.DB(q.c.dbName).C(q.c.stream).Find(queryParam).All(&result)
		if err != nil {
			return
		}
		for _, v := range result {
			s, err := json.Marshal(v)
			if err != nil {
				ch <- err.Error()
				return
			}
			ch <- string(s)
		}
	}()

	return ch
}
