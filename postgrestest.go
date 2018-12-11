// Copyright 2017 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

// Package postgrestest provides a package intended for running
// tests which require a Postgres backend.
package postgrestest

import (
	"crypto/rand"
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/lib/pq"
	"gopkg.in/errgo.v1"
)

// DB holds a connection to a schema within
// a Postgres database. The schema is created by New
// and deleted (along with all the tables) when the DB is closed.
type DB struct {
	*sql.DB
	schema string
}

// ErrDisabled is returned by New when postgres testing has
// been explicitly disabled.
var ErrDisabled = errgo.New("postgres testing is disabled")

// New connects to a Postgres instance and returns a database
// connection that uses a newly created schema with
// a random name.
// The PG* environment variables may be used to
// configure the connection parameters (see https://www.postgresql.org/docs/9.3/static/libpq-envars.html).
//
// The returned DB instance must be closed after it's finished
// with.
//
// If the environment variable PGTESTDISABLE is non-empty
// ErrDisabled will be returned.
func New() (*DB, error) {
	if os.Getenv("PGTESTDISABLE") != "" {
		return nil, ErrDisabled
	}
	name := randomSchemaName()
	db, err := sql.Open("postgres", "search_path="+name)
	if err != nil {
		return nil, errgo.Notef(err, "cannot open database")
	}
	_, err = db.Exec(`CREATE SCHEMA ` + name)
	if err != nil {
		return nil, errgo.Notef(err, "cannot create test database %q", name)
	}
	return &DB{
		DB:     db,
		schema: name,
	}, nil
}

// Close closes the database connection and removes
// the test database.
func (pg *DB) Close() error {
	// Drop the schema in a goroutine so that if it fails because some goroutine is maintaining
	// a lock on a table, we can time out instead of hanging up indefinitely
	done := make(chan error)
	go func() {
		_, err := pg.DB.Exec(fmt.Sprintf("DROP SCHEMA %q CASCADE;", pg.schema))
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			return errgo.Notef(err, "cannot drop test schema %q", pg.schema)
		}
		return nil
	case <-time.After(5 * time.Second):
		return errgo.Newf("timed out trying to drop test schema %q", pg.schema)
	}
	return nil
}

// Schema returns the test schema name.
func (pg *DB) Schema() string {
	return pg.schema
}

func randomSchemaName() string {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		panic(fmt.Errorf("cannot read random bytes: %v", err))
	}
	return fmt.Sprintf("go_test_%x", buf)
}
