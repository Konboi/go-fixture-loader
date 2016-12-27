package loader

import (
	"database/sql"
	"fmt"
	"log"
	"path"
	"regexp"
	"strings"

	"github.com/Masterminds/squirrel"
	"github.com/pkg/errors"
	"github.com/shogo82148/txmanager"
)

// FixtureLoader is XXX
type FixtureLoader struct {
	Option    *Option
	Txmanager txmanager.DB
}

// Option is loader global option
type Option struct {
	Update     bool
	Ignore     bool
	Delete     bool
	BulkInsert bool
}

// LoadOption is overlide option for loading
type LoadOption struct {
	Table  string
	Format string
	Update bool
	Ignore bool
	Delete bool
}

// Data is insert common data type
type Data struct {
	columns []string
	rows    []map[string]string // {column:value}
}

const (
	// MySQL is XXX
	MySQL = "mysql"
)

var (
	baseNameRegexp  *regexp.Regexp
	formatRegexp    *regexp.Regexp
	bulkInsertLimit = 2000
)

func init() {
	baseNameRegexp = regexp.MustCompile(`^([_A-Za-z0-9]+)`)
	formatRegexp = regexp.MustCompile(`\.([^.]*$)`)
}

// NewOption return default option
func NewOption(driver string) *Option {
	bulkInsert := false

	if driver == MySQL {
		bulkInsert = true
	}

	return &Option{
		BulkInsert: bulkInsert,
	}
}

// New is return FixtureLoader
func New(db *sql.DB, opt *Option) *FixtureLoader {
	txManager := txmanager.NewDB(db)
	if opt == nil {
		opt = NewOption("")
	}

	return &FixtureLoader{
		Option:    opt,
		Txmanager: txManager,
	}
}

// LoadFixture is load fixture
func (fl FixtureLoader) LoadFixture(value interface{}, opt *LoadOption) error {
	if opt == nil {
		opt = &LoadOption{
			Update: fl.Option.Update,
			Delete: fl.Option.Delete,
			Ignore: fl.Option.Ignore,
		}
	}

	if opt.Update && opt.Ignore {
		log.Fatalf("update and ignore are exclusive argument")
	}

	if v, ok := value.(Data); ok {
		return fl.loadFixtureFromData(v, opt)
	}

	var file string
	if v, ok := value.(string); ok {
		file = v
	} else {
		log.Fatalf("%v is not file string", value)
	}

	if opt.Table == "" {
		basename := path.Base(file)
		match := baseNameRegexp.FindStringSubmatch(basename)
		if len(match) < 2 {
			return fmt.Errorf("Please check file name")
		}
		opt.Table = match[1]
	}

	if opt.Format == "" {
		match := formatRegexp.FindStringSubmatch(file)
		if len(match) < 2 {
			return fmt.Errorf("Please check file format")
		}
		opt.Format = match[1]
	}

	var data Data
	var err error

	if opt.Format == "csv" || opt.Format == "tsv" {
		data, err = fl.getDataFromCSV(file, opt.Format)
	} else if opt.Format == "json" {
		data, err = fl.getDataFromJSON(file)
	} else if opt.Format == "yaml" || opt.Format == "yml" {
		data, err = fl.getDataFromYAML(file)
	} else {
		err = fmt.Errorf("not support format: %s", opt.Format)
	}

	if err != nil {
		return err
	}

	return fl.LoadFixture(data, opt)
}

func (fl FixtureLoader) loadFixtureFromData(data Data, opt *LoadOption) error {
	if opt.Update && opt.Ignore {
		log.Fatalln("`update` and `ignore` are exclusive option")
	}

	tx, err := fl.Txmanager.TxBegin()
	if err != nil {
		return err
	}
	defer tx.TxFinish()

	if opt.Delete {
		query, args, err := squirrel.Delete(opt.Table).ToSql()
		if err != nil {
			tx.TxRollback()
			return err
		}
		tx.Exec(query, args...)
	}

	rows := make([][]interface{}, 0, len(data.rows))
	for _, row := range data.rows {
		value := make([]interface{}, 0)
		for _, column := range data.columns {
			value = append(value, insertValue(row[column]))
		}
		rows = append(rows, value)
	}

	var query string
	var args []interface{}

	builder := squirrel.Insert(opt.Table).Columns(data.columns...)
	if fl.Option.BulkInsert {
		count := 0
		for _, value := range rows {
			builder = builder.Values(value...)
			count++
			if count > bulkInsertLimit {
				query, args, err = builder.ToSql()
				if err != nil {
					break
				}

				_, err = tx.Exec(query, args...)
				if err != nil {
					break
				}
				count = 0
				builder = squirrel.Insert(opt.Table).Columns(data.columns...)
			}
		}
		query, args, err = builder.ToSql()
		_, err = tx.Exec(query, args...)
	} else {
		for _, value := range rows {
			query, args, err = builder.Values(value...).ToSql()
			if err != nil {
				break
			}

			_, err = tx.Exec(query, args...)
			if err != nil {
				break
			}
		}
	}

	if err != nil {
		err = errors.Wrap(err, "db insert error")
		tx.TxRollback()
		return err
	}

	err = tx.TxCommit()
	if err != nil {
		return err
	}

	return nil
}

func buildOnDuplicate(columns []string, builder squirrel.InsertBuilder) squirrel.InsertBuilder {
	values := make([]string, 0, len(columns))
	for _, column := range columns {
		values = append(values, fmt.Sprintf("%s = VALUES(%s)", column, column))
	}
	suffix := fmt.Sprintf("ON DUPLICATE KEY UPDATE %s", strings.Join(values, ", "))
	return builder.Suffix(suffix)
}

func insertValue(value string) interface{} {
	if len(value) == 0 {
		return squirrel.Expr("DEFAULT")
	}

	return value
}
