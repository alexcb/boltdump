package main

import (
	"fmt"
	"os"
	"time"

	goflags "github.com/jessevdk/go-flags"
	bolt "go.etcd.io/bbolt"
)

type Flags struct {
	Tree bool `short:"t" long:"tree" description:"display results as a tree"`
	Help bool `short:"h" long:"help" description:"display this help"`
}

func main() {
	progName := "boltdump"
	if len(os.Args) > 0 {
		progName = os.Args[0]
	}
	usage := fmt.Sprintf("%s [options] <boltdb path>", progName)

	flags := Flags{}
	p := goflags.NewNamedParser("", goflags.PrintErrors|goflags.PassDoubleDash|goflags.PassAfterNonOption)
	p.AddGroup(usage, "", &flags)
	args, err := p.ParseArgs(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stdout, "failed to parse flags: %s\n", err)
		os.Exit(1)
	}
	if flags.Help {
		p.WriteHelp(os.Stdout)
		os.Exit(0)
	}

	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "%s\n", usage)
		os.Exit(1)
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

	if flags.Tree {
		dumpTree(db, nil, 2, 0)
	} else {
		dump(db, nil, sep, "")
	}
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

func dumpTree(db *bolt.DB, buk *bolt.Bucket, indent, spacing int) error {
	return db.View(func(tx *bolt.Tx) error {
		var c *bolt.Cursor
		if buk == nil {
			c = tx.Cursor()
		} else {
			c = buk.Cursor()
		}
		for k, v := c.First(); k != nil; k, v = c.Next() {
			for i := 0; i < spacing; i++ {
				fmt.Printf(" ")
			}
			if v == nil {
				var buk2 *bolt.Bucket
				if buk == nil {
					buk2 = tx.Bucket(k)
				} else {
					buk2 = buk.Bucket(k)
				}
				fmt.Printf("%q\n", string(k))
				dumpTree(db, buk2, indent, spacing+indent)
			} else {
				fmt.Printf("%q = %q\n", k, v)
			}
		}
		return nil
	})
}
