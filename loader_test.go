package loader

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"reflect"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	mysqltest "github.com/lestrrat-go/test-mysqld"
	"github.com/pkg/errors"
	"github.com/shogo82148/txmanager"
)

var (
	testMysqld *mysqltest.TestMysqld
)

type item struct {
	id   int
	name string
}

func TestNew(t *testing.T) {
	type Input struct {
		Driver  string
		Options []Option
	}

	type Output struct {
		Loader FixtureLoader
		Error  error
	}

	type Test struct {
		Title  string
		Input  Input
		Output Output
	}

	tests := []Test{
		Test{
			Title: "success: default config",
			Input: Input{
				Driver:  MySQL,
				Options: []Option{},
			},
			Output: Output{
				Loader: FixtureLoader{
					txManager: txmanager.NewDB(nil),
					driver:    MySQL,
				},
				Error: nil,
			},
		},
		Test{
			Title: "success: update option",
			Input: Input{
				Driver: MySQL,
				Options: []Option{
					Update(true),
				},
			},
			Output: Output{
				Loader: FixtureLoader{
					txManager: txmanager.NewDB(nil),
					driver:    MySQL,
					update:    true,
				},
				Error: nil,
			},
		},
		Test{
			Title: "success: delete option",
			Input: Input{
				Driver: MySQL,
				Options: []Option{
					Delete(true),
				},
			},
			Output: Output{
				Loader: FixtureLoader{
					txManager: txmanager.NewDB(nil),
					driver:    MySQL,
					delete:    true,
				},
				Error: nil,
			},
		},
		Test{
			Title: "success: use sqlite and delete option",
			Input: Input{
				Driver: "sqlite",
				Options: []Option{
					Delete(true),
				},
			},
			Output: Output{
				Loader: FixtureLoader{
					txManager: txmanager.NewDB(nil),
					driver:    "sqlite",
					delete:    true,
				},
				Error: nil,
			},
		},
		Test{
			Title: "success: update and bulk insert option",
			Input: Input{
				Driver: MySQL,
				Options: []Option{
					Update(true),
					BulkInsert(true),
				},
			},
			Output: Output{
				Loader: FixtureLoader{
					txManager:  txmanager.NewDB(nil),
					driver:     MySQL,
					update:     true,
					bulkInsert: true,
				},
				Error: nil,
			},
		},
		Test{
			Title: "error: update support only mysql",
			Input: Input{
				Driver: "sqlite",
				Options: []Option{
					Update(true),
				},
			},
			Output: Output{
				Error: fmt.Errorf("error `update` option only support mysql"),
			},
		},
		Test{
			Title: "error: set update option with ignore option",
			Input: Input{
				Driver: MySQL,
				Options: []Option{
					Update(true),
					Ignore(true),
				},
			},
			Output: Output{
				Error: fmt.Errorf("error `update` and `ignore` are exclusive option"),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.Title, func(t *testing.T) {
			l, err := New(nil, test.Input.Driver, test.Input.Options...)
			if test.Output.Error != nil {
				if errors.Cause(err).Error() != test.Output.Error.Error() {
					t.Fatalf("error invalid error message. got:%s want:%s", errors.Cause(err).Error(), test.Output.Error.Error())
				}
				return
			}

			if err != nil {
				t.Fatal("error new", err.Error())
			}

			if !reflect.DeepEqual(l, test.Output.Loader) {
				t.Fatalf("error invalid options. got:%v want:%v", l, test.Output.Loader)
			}
		})
	}
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

	type Input struct {
		File    string
		Options []Option
	}

	type Test struct {
		Title       string
		InitQueries []string
		Input       Input
		Output      []item
	}

	tests := []Test{
		Test{
			Title: "load csv",
			Input: Input{
				File:    "_data/item.csv",
				Options: []Option{},
			},
			Output: []item{
				item{id: 1, name: "エクスカリバー"},
				item{id: 2, name: "村正"},
			},
		},
		Test{
			Title: "load yml",
			Input: Input{
				File:    "_data/item.yaml",
				Options: []Option{},
			},
			Output: []item{
				item{id: 3, name: "ウィザードロッド"},
				item{id: 4, name: "ホーリーランス"},
			},
		},
		Test{
			Title: "load json",
			Input: Input{
				File:    "_data/item.json",
				Options: []Option{},
			},
			Output: []item{
				item{id: 5, name: "グラディウス"},
				item{id: 6, name: "木刀"},
			},
		},
		Test{
			Title: "load csv. add item",
			InitQueries: []string{
				"INSERT INTO item VALUES(3,'ウィザードロッド');",
				"INSERT INTO item VALUES(4,'ホーリーランス');",
			},
			Input: Input{
				File:    "_data/item.csv",
				Options: []Option{},
			},
			Output: []item{
				item{id: 1, name: "エクスカリバー"},
				item{id: 2, name: "村正"},
				item{id: 3, name: "ウィザードロッド"},
				item{id: 4, name: "ホーリーランス"},
			},
		},
		Test{
			Title: "load csv with delete option",
			InitQueries: []string{
				"INSERT INTO item VALUES(1,'エクスカリバー');",
				"INSERT INTO item VALUES(2,'村正');",
				"INSERT INTO item VALUES(3,'ウィザードロッド');",
				"INSERT INTO item VALUES(4,'ホーリーランス');",
			},
			Input: Input{
				File: "_data/item.csv",
				Options: []Option{
					Delete(true),
				},
			},
			Output: []item{
				item{id: 1, name: "エクスカリバー"},
				item{id: 2, name: "村正"},
			},
		},
		Test{
			Title: "load csv with update and table option",
			InitQueries: []string{
				"INSERT INTO item VALUES(1,'エクスカリバー');",
				"INSERT INTO item VALUES(2,'村正');",
			},
			Input: Input{
				File: "_data/item_update.csv",
				Options: []Option{
					Update(true),
					Table("item"),
				},
			},
			Output: []item{
				item{id: 1, name: "エクスカリバーNew"},
				item{id: 2, name: "村正New"},
			},
		},
		Test{
			Title: "load empty csv with delete and table option",
			InitQueries: []string{
				"INSERT INTO item VALUES(1,'エクスカリバー');",
			},
			Input: Input{
				File: "_data/zero.csv",
				Options: []Option{
					Delete(true),
					Table("item"),
				},
			},
			Output: []item{},
		},
		Test{
			Title: "load empty csv with delete and buik insert and table option",
			InitQueries: []string{
				"INSERT INTO item VALUES(1,'エクスカリバー');",
			},
			Input: Input{
				File: "_data/zero.csv",
				Options: []Option{
					Delete(true),
					BulkInsert(true),
					Table("item"),
				},
			},
			Output: []item{},
		},
	}

	fl, err := New(db, MySQL)
	if err != nil {
		t.Fatal("[error] new ", err.Error())
	}

	for _, test := range tests {
		t.Run(test.Title, func(t *testing.T) {
			defer db.Exec("TRUNCATE TABLE item;")

			if len(test.InitQueries) != 0 {
				for _, q := range test.InitQueries {
					if _, err := db.Exec(q); err != nil {
						t.Fatal("[error] init query:", err.Error())
					}
				}
			}

			if err := fl.LoadFixture(test.Input.File, test.Input.Options...); err != nil {
				t.Fatal("[error] load fixture:", err.Error())
			}

			var count int
			row := db.QueryRow("SELECT COUNT(*) FROM item")
			if err := row.Scan(&count); err != nil {
				t.Fatal("[error] select error:", err.Error())
			}

			if len(test.Output) != count {
				t.Fatalf("error load data. want:%d got:%d", len(test.Output), count)
			}

			items := []item{}
			rows, err := db.Query("select id, name from item")
			if err != nil {
				t.Fatal("[error] select error:", err.Error())
			}

			for rows.Next() {
				i := item{}
				if err := rows.Scan(&i.id, &i.name); err != nil {
					if err != sql.ErrNoRows {
						t.Fatal("error scan data.", err.Error())
					}
				}

				items = append(items, i)
			}

			if !reflect.DeepEqual(test.Output, items) {
				t.Fatalf("error load data. want:%v got:%v", test.Output, items)
			}
		})
	}
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
