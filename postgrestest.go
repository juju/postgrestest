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

	_ "github.com/lib/pq"
	"gopkg.in/errgo.v1"
)

// DB holds a connection to a Postgres database.
type DB struct {
	*sql.DB
	name string
}

// ErrDisabled is returned by New when postgres testing has
// been explicitly disabled.
var ErrDisabled = errgo.New("postgres testing is disabled")

// New connects to a Postgres instance, creates a new database
// with a random name and returns it.
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
	admindb, err := sql.Open("postgres", "dbname=postgres")
	if err != nil {
		return nil, errgo.Notef(err, "cannot open admin database")
	}
	defer admindb.Close()

	name := randomDBName()
	_, err = admindb.Exec(fmt.Sprintf("CREATE DATABASE %q;", name))
	if err != nil {
		return nil, errgo.Notef(err, "cannot create test database %q", name)
	}
	db, err := sql.Open("postgres", "dbname="+name)
	if err != nil {
		return nil, errgo.Notef(err, "cannot open test database %q", name)
	}
	return &DB{
		DB:   db,
		name: name,
	}, nil
}

// Close closes the database connection and removes
// the test database.
func (pg *DB) Close() error {
	pg.DB.Close()
	pg.DB = nil
	admindb, err := sql.Open("postgres", "dbname=postgres")
	if err != nil {
		return errgo.Notef(err, "cannot open admin DB")
	}
	defer admindb.Close()
	_, err = admindb.Exec(fmt.Sprintf("DROP DATABASE %q;", pg.name))
	if err != nil {
		return errgo.Notef(err, "cannot drop test database %q", pg.name)
	}
	return nil
}

// Name returns the name of the test database.
func (pg *DB) Name() string {
	return pg.name
}

func randomDBName() string {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		panic(fmt.Errorf("cannot read random bytes: %v", err))
	}
	return fmt.Sprintf("test_%x", buf)
}
