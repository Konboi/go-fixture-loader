# go-fixture-loader

go-fixture-loader - Loading fixtures and inserting to database

from cpan module [DBIx::FixtureLoader](https://metacpan.org/release/DBIx-FixtureLoader).

# How to use

```
package main

import (
	"database/sql"
	"log"

	"github.com/Konboi/go-fixture-loader"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	db, err := sql.Open("mysql", "root:@/test?charset=utf8")
	if err != nil {
		log.Fatalf(err.Error())
	}
	defer db.Close()

	opt := loader.NewOption(loader.MySQL)
	opt.Update = true

	opt := loader.NewOption(loader.MySQL)
	opt.Update = true
	opt.Delete = true
	fl := loader.New(cnn, opt)

	err = fl.LoadFixture("./_data/item.json", nil)
	if err != nil {
		log.Fatalln(err.Error())
	}
```
