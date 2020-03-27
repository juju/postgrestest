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
	errgo "gopkg.in/errgo.v1"
)

const defaultTimeout = 5 * time.Second

// PgTestDisable returns whether Postgres should be disabled based on the
// PGTESTDISABLE environment variable.
func PgTestDisable() bool {
	return os.Getenv("PGTESTDISABLE") != ""
}

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
//
// If the environment variable PGTESTKEEPDB is non-empty,
// the name of the test schema will be printed and the
// data will not be deleted.
//
// For optimal test performance, we recommend setting
// the following Postgres config values in your testing
// or development environment (BUT NEVER IN PRODUCTION):
//
//   fsync = off
//   synchronous_commit = off
//   full_page_writes = off
//
// Be aware that these settings may lead to data loss
// and corruption. However, they should not have any
// negative impact on ephemeral tests.
func New() (*DB, error) {
	if PgTestDisable() {
		return nil, ErrDisabled
	}
	name := randomSchemaName()
	db, err := sql.Open("postgres", "search_path="+name)
	if err != nil {
		return nil, errgo.Notef(err, "cannot open database")
	}

	err = runWithTimeout(func(done chan error) {
		_, err := db.Exec(`CREATE SCHEMA ` + name)
		done <- err
	}, defaultTimeout, "create schema")
	if err != nil {
		errClose := runWithTimeout(func(done chan error) {
			db.Close()
			done <- err
		}, defaultTimeout, "close test db after failing to create schema")
		if errClose != nil {
			return nil, errgo.Notef(errClose, "cannot create test database %q", name)
		}
		return nil, errgo.Notef(err, "cannot create test database %q", name)
	}
	return &DB{
		DB:     db,
		schema: name,
	}, nil
}

// Close removes the test database and closes the database connection. This
// method should not be called from multiple goroutines.
func (pg *DB) Close() error {
	// If for some reason someone replaced our DB with nil, there's nothing to
	// do here.
	if pg.DB == nil {
		return nil
	}

	if os.Getenv("PGTESTKEEPDB") != "" {
		fmt.Fprintf(os.Stderr, "postgrestest schema: %v\n", pg.schema)
		fmt.Fprintf(os.Stderr, "\tSET search_path TO %q;\n", pg.schema)
		fmt.Fprintf(os.Stderr, "\tDROP SCHEMA %q CASCADE;\n", pg.schema)
		return nil
	}

	// Drop the schema and close in goroutines, so that if it fails because
	// someone has a lock on something, we can time out instead of hanging up
	// indefinitely.
	err := runWithTimeout(func(done chan error) {
		_, err := pg.DB.Exec(fmt.Sprintf("DROP SCHEMA %q CASCADE;", pg.schema))
		done <- err
	}, defaultTimeout, "drop test schema "+pg.schema)
	if err != nil {
		return err
	}

	err = runWithTimeout(func(done chan error) {
		err := pg.DB.Close()
		done <- err
	}, defaultTimeout, "close test db")
	if err != nil {
		return err
	}

	return nil
}

// runWithTimeout runs toRun in a goroutine and waits for it to finish
// (up to timeout) and what describes the thing toRun is trying to accomplish
// (for nicer error messages).
func runWithTimeout(toRun func(chan error), timeout time.Duration, what string) error {
	done := make(chan error)
	go toRun(done)
	select {
	case err := <-done:
		if err != nil {
			return errgo.Notef(err, "cannot "+what)
		}
		return nil
	case <-time.After(timeout):
		return errgo.Newf("timed out trying to " + what)
	}
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
