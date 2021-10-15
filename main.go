package main

import (
	"fmt"
	"os"
	"time"

	bolt "go.etcd.io/bbolt"
)

func usage() {
	progName := "boltdump"
	if len(os.Args) > 0 {
		progName = os.Args[0]
	}

	fmt.Fprintf(os.Stdout, "usage: %s <bolt db path>\n", progName)
	os.Exit(1)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	if os.Args[1] == "--help" || os.Args[1] == "-h" {
		usage()
	}
	args := os.Args[1:]
	if os.Args[1] == "--" {
		if len(os.Args) < 3 {
			usage()
		}
		args = os.Args[2:]
	}
	dbPath := args[0]

	if _, err := os.Stat(dbPath); err != nil {
		fmt.Fprintf(os.Stdout, "failed to open %s: %s\n", dbPath, err)
		os.Exit(1)
	}

	db, err := bolt.Open(dbPath, 0400, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		fmt.Fprintf(os.Stdout, "failed to open %s: %s\n", dbPath, err)
		os.Exit(1)
	}
	defer db.Close()

	sep := "/"

	dump(db, nil, sep, "")
}

func dump(db *bolt.DB, buk *bolt.Bucket, sep, prefix string) error {
	return db.View(func(tx *bolt.Tx) error {
		var c *bolt.Cursor
		if buk == nil {
			c = tx.Cursor()
		} else {
			c = buk.Cursor()
		}
		for k, v := c.First(); k != nil; k, v = c.Next() {
			fullKey := fmt.Sprintf("%s%s%q", prefix, sep, string(k))
			if v == nil {
				var buk2 *bolt.Bucket
				if buk == nil {
					buk2 = tx.Bucket(k)
				} else {
					buk2 = buk.Bucket(k)
				}
				dump(db, buk2, sep, fullKey)
			} else {
				fmt.Printf("%s = %q\n", fullKey, v)
			}

		}
		return nil
	})
}
