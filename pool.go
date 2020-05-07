package endrMongoPool

import (
	"context"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"sync"
	"time"
)

type DBConfig struct {
	Uri            string `default:"mongodb://localhost:27017"`
	ConnectTimeout int    `default:"10"`
	DBName         string `default:""`
	PoolSize       int    `default:"5"`
	// TODO: add credentials
}

type MongoConnection struct {
	mu         sync.Mutex
	pool       chan *mongo.Database
	poolLength int
	Errors     chan error
	config     *DBConfig
}

func NewMongoConnection(dbConfig DBConfig) *MongoConnection {
	return &MongoConnection{
		pool:       make(chan *mongo.Database, dbConfig.PoolSize),
		poolLength: 0,
		Errors:     make(chan error),
		config:     &dbConfig,
	}
}

func (conn *MongoConnection) GetMongoDB() (*mongo.Database, error) {
	defer func() {
		if rec := recover(); rec != nil {
			conn.Errors <- errors.New(fmt.Sprintf("%v", rec))
		}
	}()
	if conn.config.PoolSize < 1 {
		conn.config.PoolSize = 1
	}
	conn.mu.Lock()
	if len(conn.pool) == 0 && conn.poolLength < conn.config.PoolSize {
		db, err := conn.NewMongoDB()
		if err == nil {
			conn.poolLength++
		}
		conn.mu.Unlock()
		return db, err
	}
	conn.mu.Unlock()
	for db := range conn.pool {
		if err := db.Client().Ping(context.TODO(), nil); err == nil {
			return db, nil
		}
	}
	return conn.NewMongoDB() // TODO: check quantity of connections
}

func (conn *MongoConnection) PutMongoDB(db *mongo.Database) error {
	if db == nil {
		return errors.New("nil database cannot be putted")
	}
	if err := db.Client().Ping(context.TODO(), nil); err != nil {
		return err
	}
	go func() {
		conn.pool <- db
	}()
	return nil
}

func (conn *MongoConnection) CloseMongoDB() (err error) {
	for db := range conn.pool {
		err = db.Client().Disconnect(context.TODO())
		if err != nil {
			return err
		}
	}
	return
}

func (conn *MongoConnection) NewMongoDB() (*mongo.Database, error) {
	// Create client
	mongoOptions := options.Client().ApplyURI(conn.config.Uri)
	mongoOptions = mongoOptions.SetConnectTimeout(time.Second)
	mongoOptions.SetMaxPoolSize(uint64(conn.config.ConnectTimeout))
	client, err := mongo.NewClient(mongoOptions)
	if err != nil {
		return nil, err
	}
	// Create connect
	err = client.Connect(context.TODO())
	if err != nil {
		return nil, err
	}
	// Check the connection
	err = client.Ping(context.TODO(), readpref.Secondary(readpref.WithMaxStaleness(time.Second)))
	if err != nil {
		return nil, err
	}
	fmt.Println("Connected to MongoDB!")
	return client.Database(conn.config.DBName), nil
}

func (conn *MongoConnection) DBAsync(wg *sync.WaitGroup, f func(db *mongo.Database, errChan chan error) error) (err error) {
	defer func() {
		if err != nil {
			conn.Errors <- err
		}
	}()
	if wg != nil {
		defer wg.Done()
	}
	db, err := conn.GetMongoDB()
	if err != nil {
		return
	}
	if db == nil {
		err = errors.New("cannot get db")
		return
	}
	defer func() {
		conn.PutMongoDB(db)
	}()
	err = f(db, conn.Errors)
	return
}
