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
	chanBufSize   int = 1024
	triggerSuffix     = "_capped"
)

type (
	// Connection exports mondodb connection attributes
	ConnectionT struct {
		session *mgo.Session
		dbName  string
		l       *ListennerT
		c       *CommitterT
		m       *ManageT
		q       *QueryT
	}
	// CommitterT exports Committer interface
	CommitterT struct {
		c *ConnectionT
	}
	// ListennerT export Listenner interface
	ListennerT struct {
		c      *ConnectionT
		filter map[string]Handler
		stream string
		done   chan bool
		wg     *sync.WaitGroup
	}
	// ManageT struct for Manage interface
	ManageT struct {
		c *ConnectionT
	}

	// QueryT struct for Query interface
	QueryT struct {
		c *ConnectionT
	}

	// Committer interface defines method to commit new event to eventstore
	Committer interface {
		PrepareStream(stream string) error
		SubmitEvent(stream string, sequenceID string, tag string, eventJSON string) error
		SubmitMapStringEvent(stream string, sequenceID string, tag string, body map[string]interface{}) error
	}
	// Listenner interface defines method to listen events from the datastore
	Listenner interface {
		Subscribe(stream string, fromID string) (chan string, error)
		Unsubscribe(eventChannel chan string)
	}

	// Handler type defines function which will be used as callback
	Handler func(ctx context.Context, event []interface{})
	// Listenner2 interface is replacement of Listenner
	// TODO:10 Remove Listenner interface and rename Listenner2 to Listenner
	Listenner2 interface {
		Subscribe2(stream string, eventTypes string, handlerFunc Handler) error
		Unsubscribe2(stream string, eventTypes string)
		GetLastID(stream string) string
		Listen(ctx context.Context, stream string, id string) error
	}
	// Manager interface to support internal database functions
	Manager interface {
		//DropDatabase just drop database
		//TODO:20 Remove after testing will be updated
		DropDatabase(databaseName string) error
		DatabaseNames() ([]string, error)
		CollectionNames() ([]string, error)
	}

	//Query interface to support query from database
	Query interface {
		//Find - return channel with JSON database
		Find(stream string, queryParam interface{}, sortOrder string) (chan string, error)
		FindOne(stream string, queryParam interface{}, sortOrder string) (chan string, error)
		Pipe(stream string, aggregationPipeline interface{}) (chan string, error)
	}
	//Connection interface
	Connection interface {
		Committer() Committer
		Listenner() Listenner
		Listenner2() Listenner2
		Manager() Manager
		Query() Query
		Close()
	}
)

// Dial fabric function to produce new connection to eventstore
// url - path to the mongodb server for ex. mongodb://127.0.0.1
// dbName - database name where events are stored
// stream - collection name where events are stored. EventSourcing stream
func Dial(url string, dbName string) (Connection, error) {
	var err error
	c := ConnectionT{}
	log.Println("URL to db server", url)
	c.session, err = mgo.Dial(url)
	if err != nil {
		return nil, err
	}
	c.dbName = dbName
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
func (c *ConnectionT) Close() {
	c.session.Close()
}

// Committer return committer object which implement Committer interface
func (c *ConnectionT) Committer() Committer {
	return c.c
}

// Listenner returns listenner object which implments Listenner interface
func (c *ConnectionT) Listenner() Listenner {
	c.l.wg = &sync.WaitGroup{}
	return c.l
}

// SubmitEvent submittes event to event store
func (c *CommitterT) SubmitEvent(stream string, sequenceID string, tag string, eventJSON string) error {
	var object map[string]interface{}
	err := json.Unmarshal([]byte(eventJSON), &object)
	if err != nil {
		return err
	}
	triggerStream := stream + triggerSuffix
	log.Println("trigger stream is ", triggerStream)
	//	nowString := time.Now().Format(time.RFC3339Nano)
	// := map[string]interface{}{"$date": nowString}
	event := make(map[string]interface{})
	event["sequenceID"] = sequenceID
	event["tag"] = tag
	//	event["datestamp"] = datestamp
	event["timestamp"] = time.Now()
	event["event"] = object
	log.Println("Database name is ", c.c.dbName, stream, event)
	err = c.c.session.DB(c.c.dbName).C(stream).Insert(event)
	log.Println("DBNAME ", err)
	if err != nil {
		return err
	}
	err = c.c.session.DB(c.c.dbName).C(triggerStream).Insert(bson.M{"trigger": 1})
	log.Println("Normal exit from SubmitEvent ", c.c.dbName, triggerStream, stream)
	panic("Panic from SubmitEvent")
	// return err
}

// SubmitMapString submittes event to event store
func (c *CommitterT) SubmitMapStringEvent(stream string, sequenceID string, tag string, body map[string]interface{}) error {
	triggerStream := stream + triggerSuffix
	event := make(map[string]interface{})
	event["sequenceID"] = sequenceID
	event["tag"] = tag
	event["event"] = body
	event["timestamp"] = time.Now()
	err := c.c.session.DB(c.c.dbName).C(stream).Insert(event)
	if err != nil {
		log.Println(err)
		return err
	}
	err = c.c.session.DB(c.c.dbName).C(triggerStream).Insert(bson.M{"trigger": 1})
	if err != nil {
		log.Println(err)
	}
	return err
}

func (c *CommitterT) PrepareStream(stream string) error {
	triggerStream := stream + triggerSuffix
	log.Println("Enter PrepareStream")
	collections, err := c.c.session.DB(c.c.dbName).CollectionNames()
	if err != nil {
		return err
	}
	if !contains(collections, triggerStream) {
		cInfo := mgo.CollectionInfo{
			Capped:   true,
			MaxBytes: 1000000,
		}
		err = c.c.session.DB(c.c.dbName).C(triggerStream).Create(&cInfo)
		log.Println("triggerStream created ", c.c.dbName, triggerStream)
	}
	log.Println("Exit PrepareStream with error ", err)
	return err
}

// ReadEvents Read JSON events started from fromId to slice of strings
func (c *ConnectionT) readEvents(stream string, fromID string) (*mgo.Iter, error) {
	var (
		iter *mgo.Iter
	)
	if fromID != "" {
		if !bson.IsObjectIdHex(fromID) {
			return nil, errors.New("Error: Incorrect fromID " + fromID)
		}
		objID := bson.ObjectIdHex(fromID)
		iter = c.session.DB(c.dbName).C(stream).Find(bson.M{"_id": bson.M{"$gt": objID}}).Iter()
	} else {
		iter = c.session.DB(c.dbName).C(stream).Find(nil).Iter()
	}
	return iter, nil
}

// Subscribe returns channel from event store
func (e *ListennerT) Subscribe(stream string, fromID string) (chan string, error) {
	if e.c.session == nil {
		return nil, errors.New("Mongo isn't connected. Please use Dial().")
	}
	triggerStream := stream + triggerSuffix
	cTrigger := e.c.session.DB(e.c.dbName).C(triggerStream)
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
				evIter, err := e.c.readEvents(stream, lastEventID)
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
func (c *ConnectionT) Listenner2() Listenner2 {
	c.l.wg = &sync.WaitGroup{}
	return c.l
}

// Subscribe2 - new implementation of Subscribe fucntion. Will replace Subscribe
func (e *ListennerT) Subscribe2(stream string, eventType string, handlerFunc Handler) error {
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
func (e *ListennerT) Unsubscribe2(stream string, eventType string) {
	return
}

// GetLastID - returns last event id
func (e *ListennerT) GetLastID(stream string) string {
	var result map[string]interface{}
	iter := e.c.session.DB(e.c.dbName).C(stream).Find(nil).Sort("-$natural").Limit(1).Iter()
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
func (e *ListennerT) Listen(ctx context.Context, stream string, id string) error {
	if e.c.session == nil {
		return errors.New("Mongo isn't connected. Please use Dial().")
	}
	for filter, handler := range e.filter {
		go e.processSubscription(ctx, stream, filter, id, handler)
	}
	<-ctx.Done()
	return nil
}
func (c *ConnectionT) readEventsLimit(stream string, filter string, fromID string, limit int) ([]interface{}, error) {
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
	err = sessionCopy.DB(c.dbName).C(stream).Find(q).Limit(limit).All(&result)
	return result, err
}

func (e *ListennerT) processSubscription(ctx context.Context, stream string, filter string, id string, handler Handler) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Handler function panic detected. Recovering", r)
		}
	}()
	for {
		result, err := e.c.readEventsLimit(stream, filter, id, 1000)
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
func (c *ConnectionT) Manager() Manager {
	return c.m
}

// DropDatabase - drop Mongo Database
func (m *ManageT) DropDatabase(databaseName string) error {
	return m.c.session.DB(databaseName).DropDatabase()
}

func (m *ManageT) DatabaseNames() ([]string, error) {
	return m.c.session.DatabaseNames()
}

func (m *ManageT) CollectionNames() ([]string, error) {
	return m.c.session.DB(m.c.dbName).CollectionNames()
}

func (c *ConnectionT) Query() Query {
	return c.q
}

// Query request data from database
func (q *QueryT) Find(stream string, queryParam interface{}, sortOrder string) (chan string, error) {
	ch := make(chan string, 256)

	go func() {
		var (
			result interface{}
			iter   *mgo.Iter
		)
		defer close(ch)
		if sortOrder != "" {
			iter = q.c.session.DB(q.c.dbName).C(stream).Find(queryParam).Sort(sortOrder).Iter()
		} else {
			iter = q.c.session.DB(q.c.dbName).C(stream).Find(queryParam).Iter()
		}
		if iter == nil {
			return
		}
		defer iter.Close()
		if iter.Err() != nil {
			ch <- iter.Err().Error()
			return
		}
		for iter.Next(&result) {
			s, err := json.Marshal(result)
			if err != nil {
				ch <- err.Error()
				return
			}
			ch <- string(s)
		}
	}()

	return ch, nil
}

// Query request data from database
func (q *QueryT) FindOne(stream string, queryParam interface{}, sortOrder string) (chan string, error) {
	ch := make(chan string, 256)

	go func() {
		var (
			result interface{}
			iter   *mgo.Iter
		)
		defer func() {
			close(ch)
		}()
		if sortOrder != "" {
			iter = q.c.session.DB(q.c.dbName).C(stream).Find(queryParam).Sort(sortOrder).Limit(1).Iter()
		} else {
			iter = q.c.session.DB(q.c.dbName).C(stream).Find(queryParam).Limit(1).Iter()
		}
		if iter == nil {
			return
		}
		defer iter.Close()
		if iter.Err() != nil {
			ch <- iter.Err().Error()
			return
		}
		for iter.Next(&result) {
			s, err := json.Marshal(result)
			if err != nil {
				ch <- err.Error()
				return
			}
			ch <- string(s)
			break
		}
	}()

	return ch, nil
}
func (q *QueryT) Pipe(stream string, aggregationPipeline interface{}) (chan string, error) {
	ch := make(chan string, 256)

	go func() {
		var (
			result interface{}
			iter   *mgo.Iter
		)
		defer close(ch)
		iter = q.c.session.DB(q.c.dbName).C(stream).Pipe(aggregationPipeline).Iter()
		defer iter.Close()
		if iter.Err() != nil {
			ch <- iter.Err().Error()
			return
		}
		for iter.Next(&result) {
			s, err := json.Marshal(result)
			if err != nil {
				ch <- err.Error()
				return
			}
			ch <- string(s)
		}
	}()

	return ch, nil
}
