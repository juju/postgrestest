// Copyright 2017 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package postgrestest_test

import (
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/juju/postgrestest"
)

func TestNew(t *testing.T) {
	c := qt.New(t)
	db, err := postgrestest.New()
	c.Assert(err, qt.Equals, nil)
	name := db.Name()
	// Check that we can actually use it.
	_, err = db.Exec(`CREATE TABLE x (id text, val text)`)
	c.Assert(err, qt.Equals, nil)
	_, err = db.Exec(`INSERT INTO x (id, val) VALUES ('a', 'b')`)
	c.Assert(err, qt.Equals, nil)
	row := db.QueryRow(`SELECT val FROM x WHERE id = 'a'`)
	var val string
	c.Assert(row.Scan(&val), qt.Equals, nil)
	c.Assert(val, qt.Equals, "b")
	err = db.Close()
	c.Assert(err, qt.Equals, nil)

	// Check that we can't open the database any more.
	sdb, err := sql.Open("postgres", "dbname="+name)
	if err == nil {
		// Open doesn't necessarily verify that the database
		// is open, so Ping it to make sure.
		err = sdb.Ping()
	}
	c.Assert(err, qt.ErrorMatches, `.*database .* does not exist`, qt.Commentf("name %q", name))
}
