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

	schema := db.Schema()
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

	// Connect again and check that the schema has been deleted.
	sdb, err := sql.Open("postgres", "")
	c.Assert(err, qt.Equals, nil)
	defer sdb.Close()

	row = sdb.QueryRow(`SELECT COUNT(nspname) FROM pg_namespace WHERE nspname = '` + schema + `'`)
	var count int
	c.Assert(row.Scan(&count), qt.Equals, nil)
	c.Assert(count, qt.Equals, 0)
}
