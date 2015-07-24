package main

import (
	"encoding/json"
	"flag"
	"fmt"
	r "github.com/dancannon/gorethink"
	"github.com/jeffail/tunny"
	"io"
	"log"
	"os"
	"time"
)

const BMO_ART = `
    ▄██████████████████████▄
    █                      █
    █ ▄██████████████████▄ █
    █ █                  █ █
    █ █                  █ █
    █ █  █            █  █ █
    █ █     ▄▄▄▄▄▄▄▄     █ █
    █ █     ▀▄    ▄▀     █ █
    █ █       ▀▀▀▀       █ █
    █ █                  █ █
 █▌ █ ▀██████████████████▀ █ ▐█
 █  █                      █  █
 █  █ ████████████     ██  █  █
 █  █                      █  █
 █  █               ▄      █  █
 ▀█▄█   ▐█▌       ▄███▄ ██ █▄█▀
   ▀█  █████               █▀
    █   ▐█▌         ▄██▄   █
    █              ▐████▌  █
    █ ▄▄▄ ▄▄▄       ▀██▀   █
    █                      █
    ▀██████████████████████▀
        ██            ██
        ██            ██
        ██            ██
        ██            ██
       ▐██            ██▌`

const POOL_SIZE = 20

type BMO struct {
	address  string
	database string
	table    string
	seq      uint64
}

func NewBMO(address string, database string, table string) *BMO {
	bmo := &BMO{
		address:  address,
		database: database,
		table:    table,
		seq:      0,
	}
	return bmo
}

type Message struct {
	Time int64       `gorethink:"t"`
	Seq  uint64      `gorethink:"i"`
	Obj  interface{} `gorethink:"o"`
}

func (bmo *BMO) Compute(input *os.File) {

	var err error
	var cur *r.Cursor
	var session *r.Session

	// set up database connection pool
	session, err = r.Connect(r.ConnectOpts{
		Address:       bmo.address,
		Database:      bmo.database,
		DiscoverHosts: true,
	})
	session.SetMaxOpenConns(POOL_SIZE)
	if err != nil {
		log.Fatalln(err)
	}
	// ensure table is present
	var tableNames []string
	cur, err = r.DB(bmo.database).TableList().Run(session)
	if err != nil {
		log.Fatalln(err)
	}
	cur.All(&tableNames)
	set := make(map[string]bool)
	for _, v := range tableNames {
		set[v] = true
	}
	if !set[bmo.table] {
		log.Println("Creating table ", bmo.table)
		_, err = r.DB(bmo.database).TableCreate(bmo.table).RunWrite(session)
		if err != nil {
			log.Fatal("Error creating table: ", err)
			os.Exit(1)
		}
	}

	// deliver the messages
	decoder := json.NewDecoder(input)
	m := &Message{}

	pool, _ := tunny.CreatePoolGeneric(POOL_SIZE).Open()
	defer pool.Close()

	for {
		err = decoder.Decode(&m.Obj)
		m.Time = time.Now().UnixNano() / 1000000 // ms
		m.Seq = bmo.seq

		switch {
		case err == io.EOF:
			return
		case err != nil:
			log.Fatal("Can't parse json input, \"", err, "\". Object #", bmo.seq, ", after ", m.Obj)
			os.Exit(1)
		}

		pool.SendWork(func() {
			_, err = r.Table(bmo.table).Insert(m).RunWrite(session)
			if err != nil {
				log.Fatal(err)
				os.Exit(1)
			}
		})

		bmo.seq += 1
	}
}

func main() {

	table := flag.String("table", "bmo_test", "Name of target table")
	address := flag.String("address", "127.0.0.1", "RethinkDB host[:port]")
	database := flag.String("database", "sophia", "Name of target database")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s\n\nUsage of %s:\n", BMO_ART, os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()

	bmo := NewBMO(*address, *database, *table)
	bmo.Compute(os.Stdin)

}
