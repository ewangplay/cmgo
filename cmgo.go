package cmgo

import (
    "fmt"
    "gopkg.in/mgo.v2"
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

    if host == "" {
        return nil, fmt.Errorf("host addr not set")
    }

    var mongoAddr string
    if username != "" && password != "" && port != "" && dbname != "" {
        mongoAddr = fmt.Sprintf("mongodb://%v:%v@%v:%v/%v", username, password, host, port, dbname)
    } else if username != "" && password != "" && port != "" {
        mongoAddr = fmt.Sprintf("mongodb://%v:%v@%v:%v", username, password, host, port)
    } else if username != "" && password != "" {
        mongoAddr = fmt.Sprintf("mongodb://%v:%v@%v", username, password, host)
    } else if port != "" && dbname != "" {
        mongoAddr = fmt.Sprintf("mongodb://%v:%v/%v", host, port, dbname)
    } else if port != "" {
        mongoAddr = fmt.Sprintf("mongodb://v:%v", host, port)
    } else {
        mongoAddr = fmt.Sprintf("mongodb://v", host)
    }

    // 连接数据库
    session, err := mgo.Dial(mongoAddr)
    if err != nil {
        return nil, err
    }

    mgoClient.session = session

    if dbname != "" {
        mgoClient.database = session.DB(dbname)
    }

    return mgoClient, nil
}

func (this *MGOClient) Close() {
    this.session.Close()
}

//if the dbname is empty, the "test" database is selected.
func (this *MGOClient) UseDB(dbname string) error {
    if this.session == nil {
        return fmt.Errorf("invalid session object")
    }

    this.database = this.session.DB(dbname)
    return nil
}

func (this *MGOClient) UseCollection(collection_name string) error {
    if this.database == nil {
        fmt.Errorf("invalid database object")
    }

    this.collection = this.database.C(collection_name)
    return nil
}

func (this *MGOClient) GetCurrentCollection() *mgo.Collection {
    return this.collection
}

func (this *MGOClient) Insert(doc interface{}) error {
    if this.collection == nil {
        return fmt.Errorf("invalid collections object")
    }

    return this.collection.Insert(doc)
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

