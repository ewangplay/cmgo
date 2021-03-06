# cmgo

A simple mgo client to use mongodb

# Example

Example [program](examples/example.go) demonstrates how to use the cmgo.

```go
package main

import (
    "fmt"
	"github.com/ewangplay/cmgo"
    "gopkg.in/mgo.v2/bson"
)

type Mail struct {
    Id    bson.ObjectId "_id"
    Name  string
    Email string
}

func main () {
    mgoClient, err := cmgo.New("127.0.0.1", "", "", "", "")
    if err != nil {
        panic(err)
    }
    defer mgoClient.Close()

    mgoClient.UseDB("test")
    mgoClient.UseCollection("test")

    m1 := Mail{bson.NewObjectId(), "user1", "user1@sample.com"}
    m2 := Mail{bson.NewObjectId(), "user2", "user2@sample.com"}

    err = mgoClient.Insert(&m1)
    err = mgoClient.Insert(&m2)

    var ms Mail
    err = mgoClient.Find(&bson.M{"name": "user2"}).One(&ms)
    if err != nil {
        panic(err)
    }

    fmt.Println(ms)
}
```

# Install

`go get github.com/ewangplay/cmgo`

Use `go get -u` to update the package.

# Tests

Run `go test cmgo` to run test on cmgo
and `go bench` for benchmarks

## License

The [BSD 3-Clause license][bsd], the same as the [Go language][golang].
[bsd]: http://opensource.org/licenses/BSD-3-Clause
[golang]: http://golang.org/LICENSE
