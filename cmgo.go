package cmgo

import (
    "fmt"
    "gopkg.in/mgo.v2"
    "gopkg.in/mgo.v2/bson"
)

type MGOClient struct {
    session *mgo.Session
    database *mgo.Database
    collection *mgo.Collection
}

func New(host, port, username, password, dbname string) (*MGOClient, error) {
    mgoClient := &MGOClient{
        session: nil,
        database: nil,
        collection: nil,
    }

    mongoAddr := fmt.Sprintf("mongodb://%v:%v@%v:%v/%v", username, password, host, port, dbname)

    // 连接数据库
    session, err := mgo.Dial(mongoAddr)
    if err != nil {
        return nil, err
    }

    mgoClient.session = session
    mgoClient.database = session.DB(dbname)

    return mgoClient, nil
}

func (this *MGOClient) Close() {
    this.session.Close()
}

func (this *MGOClient) UseCollection(collection_name string) {
    this.collection = this.database.C(collection_name)
}

func (this *MGOClient) Insert(docs ...interface{}) error {
    if this.collection == nil {
        return fmt.Errorf("invalid collections object")
    }

    return this.collection.Insert(docs)
}

func (this *MGOClient) Remove(selector interface{}) error {
    if this.collection == nil {
        return fmt.Errorf("invalid collections object")
    }

    return this.collection.Remove(selector)
}

func (this *MGOClient) RemoveAll(selector interface{}) error {
    if this.collection == nil {
        return fmt.Errorf("invalid collections object")
    }
    
    _, err := this.collection.RemoveAll(selector)
    return err
}

func (this *MGOClient) Update(selector interface{}, update interface{}) error {
    if this.collection == nil {
       return  fmt.Errorf("invalid collections object")
    }

    return this.collection.Update(selector, update)
}

func (this *MGOClient) UpdateAll(selector interface{}, update interface{}) error {
    if this.collection == nil {
        return fmt.Errorf("invalid collections object")
    }

    _, err := this.collection.UpdateAll(selector, update)

    return err
}

func (this *MGOClient) Upsert(selector interface{}, update interface{}) error {
    if this.collection == nil {
        return fmt.Errorf("invalid collections object")
    }

    _, err := this.collection.Upsert(selector, update)

    return err
}

func (this *MGOClient) Find(query interface{}) *mgo.Query {
    if this.collection == nil {
        return nil
    }

    return this.collection.Find(query)
}

func (this *MGOClient) DropCollection() error {
    if this.collection == nil {
        return fmt.Errorf("invalid collections object")
    }

    return this.collection.DropCollection()
}

func (this *MGOClient) Count() (n int, err error) {
    if this.collection == nil {
        return 0, fmt.Errorf("invalid collections object")
    }

    return this.collection.Count()
}

