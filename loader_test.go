package loader

import (
	"database/sql"
	"log"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/lestrrat/go-test-mysqld"
)

var (
	testMysqld *mysqltest.TestMysqld
)

type item struct {
	id   int
	name string
}

func TestLoadFixrure(t *testing.T) {
	db, err := sql.Open("mysql", testMysqld.Datasource("test", "", "", 0))
	if err != nil {
		t.Fatal("[error] db connection", err.Error())
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE item (id INTEGER PRIMARY KEY, name VARCHAR(255)) DEFAULT CHARACTER SET utf8mb4;")
	if err != nil {
		t.Fatal("[error] create table", err.Error())
	}

	fl := New(db, NewOption(""))
	err = fl.LoadFixture("_data/item.csv", nil)
	if err != nil {
		t.Fatal("[error] load fixture:", err.Error())
	}

	var count int
	row := db.QueryRow("SELECT COUNT(*) FROM item")
	err = row.Scan(&count)
	if err != nil {
		t.Fatal("[error] select error:", err.Error())
	}

	if count != 2 {
		t.Fatal("[error] item.csv load error")
	}

	items := make([]item, 0)
	rows, err := db.Query("SELECT * FROM item ORDER BY id")
	if err != nil {
		t.Fatal("[error] select error:", err.Error())
	}
	for rows.Next() {
		item := item{}
		err = rows.Scan(&item.id, &item.name)
		if err != nil {
			t.Fatal("[error] scan rows:", err.Error())
		}
		items = append(items, item)
	}

	if items[0].name != "エクスカリバー" {
		t.Fatalf("[error] item.csv load error: %v", items)
	}

	t.Run("adding yaml", func(t *testing.T) {
		fl.LoadFixture("_data/item.yaml", nil)

		var count int
		row := db.QueryRow("SELECT COUNT(*) FROM item")
		err = row.Scan(&count)
		if err != nil {
			t.Fatal("[error] select error:", err.Error())
		}

		if count != 4 {
			t.Fatal("[error] item.json load error")
		}

		items := make([]item, 0)
		rows, err := db.Query("SELECT * FROM item ORDER BY id")
		if err != nil {
			t.Fatal("[error] select error:", err.Error())
		}
		for rows.Next() {
			item := item{}
			err = rows.Scan(&item.id, &item.name)
			if err != nil {
				t.Fatal("[error] scan rows:", err.Error())
			}
			items = append(items, item)
		}

		if items[3].name != "ホーリーランス" {
			t.Fatalf("[error] item.csv load error: %v", items)
		}

	})

	t.Run("adding json", func(t *testing.T) {
		fl.LoadFixture("_data/item.json", nil)

		var count int
		row := db.QueryRow("SELECT COUNT(*) FROM item")
		err = row.Scan(&count)
		if err != nil {
			t.Fatal("[error] select error:", err.Error())
		}

		if count != 6 {
			t.Fatal("[error] item.json load error")
		}

		items := make([]item, 0)
		rows, err := db.Query("SELECT * FROM item ORDER BY id")
		if err != nil {
			t.Fatal("[error] select error:", err.Error())
		}
		for rows.Next() {
			item := item{}
			err = rows.Scan(&item.id, &item.name)
			if err != nil {
				t.Fatal("[error] scan rows:", err.Error())
			}
			items = append(items, item)
		}

		if items[5].name != "木刀" {
			t.Fatalf("[error] item.csv load error: %v", items)
		}
	})
}

func TestMain(m *testing.M) {
	mysqld, err := mysqltest.NewMysqld(nil)
	if err != nil {
		log.Fatalf("[error] setup test db: %s", err.Error())
	}

	db, err := sql.Open("mysql", mysqld.DSN(mysqltest.WithDbname("")))
	if err != nil {
		log.Fatalf("[error] connect db %s :%s", mysqld.DSN(mysqltest.WithDbname("")), err.Error())
	}

	// MySQL over 5.7 not exists test database
	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS test")
	if err != nil {
		log.Fatalf("[error] create test db %s", err.Error())
	}

	testMysqld = mysqld
	defer mysqld.Stop()

	code := m.Run()
	os.Exit(code)
}
