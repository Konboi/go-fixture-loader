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
	driver     string
	txManager  txmanager.DB
	update     bool
	ignore     bool
	delete     bool
	bulkInsert bool
	// Load Option
	table  string
	format string
}

// Option is set load option
type Option func(*FixtureLoader) error

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

// Update is update data when exists duplicate data
func Update(update bool) Option {
	return func(f *FixtureLoader) error {
		if update && f.ignore {
			return errors.New("error `update` and `ignore` are exclusive option")
		}

		if update && f.driver != MySQL {
			return errors.New("error `update` option only support mysql")
		}

		f.update = update

		return nil
	}
}

// Ignore skip insert when exists duplicate data
func Ignore(ignore bool) Option {
	return func(f *FixtureLoader) error {
		if f.update && ignore {
			return errors.New("error `update` and `ignore` are exclusive option")
		}

		f.ignore = ignore

		return nil
	}
}

// Delete is delete all data before insert data.
func Delete(delete bool) Option {
	return func(f *FixtureLoader) error {
		f.delete = delete
		return nil
	}
}

// BulkInsert is insert data multi
func BulkInsert(bulk bool) Option {
	return func(f *FixtureLoader) error {
		f.bulkInsert = bulk
		return nil
	}
}

// Table set insert table name
func Table(table string) Option {
	return func(f *FixtureLoader) error {
		f.table = table

		return nil
	}
}

// Format set insert file format
func Format(format string) Option {
	return func(f *FixtureLoader) error {
		f.format = format

		return nil
	}
}

// New is return FixtureLoader
func New(db *sql.DB, driver string, options ...Option) (FixtureLoader, error) {
	txManager := txmanager.NewDB(db)

	fl := FixtureLoader{
		txManager: txManager,
		driver:    driver,
	}

	for _, option := range options {
		if err := option(&fl); err != nil {
			return FixtureLoader{}, errors.Wrap(err, "error invalid option")
		}
	}

	return fl, nil
}

// LoadFixture is load fixture
func (fl FixtureLoader) LoadFixture(value interface{}, options ...Option) error {

	f := fl

	for _, option := range options {
		if err := option(&f); err != nil {
			return errors.Wrap(err, "error invalid option")
		}
	}

	if v, ok := value.(Data); ok {
		return f.loadFixtureFromData(v, options...)
	}

	var file string
	if v, ok := value.(string); ok {
		file = v
	} else {
		log.Fatalf("%v is not file string", value)
	}

	if f.table == "" {
		basename := path.Base(file)
		match := baseNameRegexp.FindStringSubmatch(basename)
		if len(match) < 2 {
			return fmt.Errorf("Please check file name")
		}
		f.table = match[1]
	}

	if f.format == "" {
		match := formatRegexp.FindStringSubmatch(file)
		if len(match) < 2 {
			return fmt.Errorf("Please check file format")
		}
		f.format = match[1]
	}

	var data Data
	var err error

	if f.format == "csv" || f.format == "tsv" {
		data, err = f.getDataFromCSV(file, f.format)
	} else if f.format == "json" {
		data, err = f.getDataFromJSON(file)
	} else if f.format == "yaml" || f.format == "yml" {
		data, err = f.getDataFromYAML(file)
	} else {
		err = fmt.Errorf("not support format: %s", f.format)
	}

	if err != nil {
		return err
	}

	return f.LoadFixture(data, options...)
}

func (fl FixtureLoader) loadFixtureFromData(data Data, options ...Option) error {
	f := fl
	for _, option := range options {
		if err := option(&f); err != nil {
			return errors.Wrap(err, "error invalid option")
		}
	}

	tx, err := f.txManager.TxBegin()
	if err != nil {
		return err
	}
	defer tx.TxFinish()

	if f.delete {
		query, args, err := squirrel.Delete(quote(f.table)).ToSql()
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

	quotedColumns := make([]string, len(data.columns))
	for i, c := range data.columns {
		quotedColumns[i] = quote(c)
	}

	builder := squirrel.Insert(quote(f.table)).Columns(quotedColumns...)
	if f.update {
		builder = buildOnDuplicate(quotedColumns, builder)
	}

	if f.bulkInsert {
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
				builder = squirrel.Insert(quote(f.table)).Columns(quotedColumns...)
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

	if err := tx.TxCommit(); err != nil {
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

// TODO: Make it changeable for each driver
func quote(s string) string {
	return fmt.Sprintf("`%s`", s)
}
