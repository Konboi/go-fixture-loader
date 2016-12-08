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

	_, err = db.Exec("CREATE TABLE item (id INTEGER PRIMARY KEY, name VARCHAR(255));")
	if err != nil {
		t.Fatal("[error] create table", err.Error())
	}

	fl := New(db, Option{})
	err = fl.LoadFixture("_data/item.csv", Option{})
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
		t.Fatal("[error] item.csv load error: %v", items)
	}

	defer truncateTable()
}

func truncateTable() {
	db, err := sql.Open("mysql", testMysqld.Datasource("test", "", "", 0))
	if err != nil {
		log.Fatal("db connection error:", err.Error())
	}

	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		log.Fatal("[error] SHOW TABLES:", err.Error())
	}

	for rows.Next() {
		var table string
		err := rows.Scan(&table)
		if err != nil {
			log.Fatal("[error] scan table", err.Error())
		}
		log.Println(table)
		_, err = db.Exec("TRUNCATE TABLE " + table)
		if err != nil {
			log.Fatal("[error] truncate table:", err.Error())
		}
	}
	defer db.Close()
}

func TestMain(m *testing.M) {
	mysqld, err := mysqltest.NewMysqld(nil)
	if err != nil {
		log.Fatalf("[error] setup test db: %s", err.Error())
	}
	testMysqld = mysqld
	defer mysqld.Stop()

	code := m.Run()
	os.Exit(code)
}
