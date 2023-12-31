package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"flag"
	"fmt"
	"github.com/marcboeker/go-duckdb"
	"log"
	"net"
	"os"
)

var options struct {
	listenAddress string
	duckDBFile    string
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage:  %s [options]\n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <duckdb-file>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Example: %s somefile.db\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "         %s \"somefile.db?access_mode=read_only&threads=4\"\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.StringVar(&options.listenAddress, "listen", "127.0.0.1:15432", "Listen address")
	flag.Parse()

	if flag.NArg() > 0 {
		options.duckDBFile = flag.Arg(0)
	} else {
		fmt.Fprintf(os.Stderr, "Error: DuckDB file is required.\n\n")
		flag.Usage()
		os.Exit(1)
	}

	ln, err := net.Listen("tcp", options.listenAddress)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Listening on", ln.Addr())

	//
	connector, err := duckdb.NewConnector(options.duckDBFile, func(execer driver.ExecerContext) error {
		bootQueries := []string{
			"INSTALL 'json'",
			"LOAD 'json'",
			"INSTALL 'icu'",
			"LOAD 'icu'",
		}

		for _, qry := range bootQueries {
			_, err = execer.ExecContext(context.Background(), qry, nil)
			if err != nil {
				return err
			}
		}
		return nil
	})

	//conn, err := connector.Connect(context.Background())
	//if err != nil {
	//	panic(err)
	//}

	//db, err := sql.Open("duckdb", "duckdb.db?access_mode=READ_WRITE")
	db := sql.OpenDB(connector)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	db.Ping()

	setting := db.QueryRow("SELECT current_setting('access_mode')")
	var am string
	setting.Scan(&am)
	log.Printf("DB opened with access mode %s", am)
	ctx, cancel := context.WithCancel(context.Background())

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}
		log.Println("Accepted connection from", conn.RemoteAddr())

		b := NewDuckDbBackend(conn, db, ctx, cancel)
		go func() {
			err := b.Run()
			if err != nil {
				log.Println(err)
			}
			log.Println("Closed connection from", conn.RemoteAddr())
		}()
	}
}
