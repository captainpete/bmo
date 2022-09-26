package main

import (
	"encoding/json"
	"flag"
	"fmt"
	r "github.com/dancannon/gorethink"
	"github.com/Jeffail/tunny"
	"io"
	"log"
	"os"
	"runtime"
	"strings"
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

// http://www.rethinkdb.com/docs/troubleshooting/
// "RethinkDB operates at peak performance when the batch size is around two hundred documents."
const INSERT_BATCH_SIZE = 200

type BMO struct {
	nodes    NodeList
	database string
	table    string
	seq      uint64
}

func NewBMO(nodes NodeList, database string, table string) *BMO {
	bmo := &BMO{
		nodes:    nodes,
		database: database,
		table:    table,
		seq:      0,
	}
	return bmo
}

type Message interface{}

func (bmo *BMO) Compute(input *os.File) {

	var err error
	var cur *r.Cursor
	var session *r.Session

	// set up database connection pool
	session, err = r.Connect(r.ConnectOpts{
		Addresses:     bmo.nodes,
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
			log.Fatalln("Error creating table: ", err)
			os.Exit(1)
		}
	}

	// deliver the messages
	decoder := json.NewDecoder(input)
	ms := make([]Message, INSERT_BATCH_SIZE)
	var m *Message
	var i uint64
	table := r.Table(bmo.table)
	insertOptions := r.InsertOpts{Durability: "soft"}

	pool := tunny.NewFunc(POOL_SIZE, func(data interface{}) interface{} {
		// fmt.Fprintf(os.Stderr, "Writing: %s\n", data)

		_, err = table.Insert(data, insertOptions).RunWrite(session)
		if err != nil {
			log.Fatal(err)
			os.Exit(1)
		}

		return true
	})
	defer pool.Close()

	for {
		i = bmo.seq % INSERT_BATCH_SIZE
		j := i
		m = &ms[i]
		err = decoder.Decode(&m)

		switch {
		case err == io.EOF:
			pool.Process(ms[:j])
			return
		case err != nil:
			pool.Process(ms[:j])
			log.Fatal("Can't parse json input, \"", err, "\". Object #", bmo.seq, ", after ", m)
			os.Exit(1)
		default:
			if i+1 == INSERT_BATCH_SIZE {
				j += 1
				pool.Process(ms[:j])
			}
		}

		bmo.seq += 1
	}
}

type NodeList []string

func (list *NodeList) String() string {
	return strings.Join(*list, ", ")
}

func (list *NodeList) Set(node string) error {
	*list = append(*list, node)
	return nil
}

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	var nodes NodeList
	flag.Var(&nodes, "node", "RethinkDB host[:port], can specify multiple times")
	database := flag.String("database", "sophia", "Name of target database")
	table := flag.String("table", "bmo_test", "Name of target table")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s\n\nUsage of %s:\n", BMO_ART, os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()

	if len(nodes) == 0 {
		log.Fatalln("Specify at least one node address")
		flag.Usage()
		os.Exit(2)
	}
	if *database == "" {
		log.Fatalln("Specify the database")
		flag.Usage()
		os.Exit(2)
	}
	if *table == "" {
		log.Fatalln("Specify the target table")
		flag.Usage()
		os.Exit(2)
	}

	bmo := NewBMO(nodes, *database, *table)
	bmo.Compute(os.Stdin)

}
