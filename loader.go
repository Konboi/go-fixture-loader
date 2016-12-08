package loader

import (
	"database/sql"
	"fmt"
	"log"
	"path"
	"regexp"
	"strings"

	"github.com/Masterminds/squirrel"
	"github.com/shogo82148/txmanager"
)

// FixtureLoader is XXX
type FixtureLoader struct {
	option    option
	db        *sql.DB
	txmanager txmanager.DB
}

type option struct {
	update     bool
	ignore     bool
	delete     bool
	bulkInsert bool
	table      string
	format     string
}

// Option is XXX
type Option map[string]interface{}

type data struct {
	columns []string
	rows    []map[string]string
}

var (
	baseNameRegexp *regexp.Regexp
	formatRegexp   *regexp.Regexp
)

func init() {
	baseNameRegexp = regexp.MustCompile(`^([_A-Za-z0-9]+)`)
	formatRegexp = regexp.MustCompile(`\.([^.]*$)`)
}

// New is return FixtureLoader
func New(db *sql.DB, opt Option) FixtureLoader {
	txManager := txmanager.NewDB(db)

	option := parseOption(opt)

	return FixtureLoader{
		option:    option,
		db:        db,
		txmanager: txManager,
	}
}

// LoadFixture is load fixture
func (fl FixtureLoader) LoadFixture(file string, opt Option) error {
	option := fl.option

	if update, ok := opt["update"].(bool); ok {
		option.update = update
	}
	if delete, ok := opt["delete"].(bool); ok {
		option.delete = delete
	}
	if ignore, ok := opt["ignore"].(bool); ok {
		option.ignore = ignore
	}

	if option.update && option.ignore {
		log.Fatalf("update and ignore are exclusive argument")
	}

	if table, ok := opt["table"].(string); !ok || table == "" {
		basename := path.Base(file)
		match := baseNameRegexp.FindStringSubmatch(basename)
		if len(match) < 2 {
			fmt.Errorf("Please check file name")
		}
		option.table = match[1]
	}

	if format, ok := opt["format"].(string); !ok || format == "" {
		match := formatRegexp.FindStringSubmatch(file)
		if len(match) < 2 {
			fmt.Errorf("Please check file format")
		}
		option.format = match[1]
	}

	var data data
	var err error
	if option.format == "csv" || option.format == "tsv" {
		data, err = fl.getDataFromCSV(file, option.format)
	} else if option.format == "json" {
		data, err = fl.getDataFromJSON(file)
	} else if option.format == "yaml" || option.format == "yml" {
		data, err = fl.getDataFromYAML(file)
	} else {
		err = fmt.Errorf("not support format: %s", option.format)
	}

	if err != nil {
		return err
	}

	err = fl.loadFixtureFromData(data, option)
	if err != nil {
		return err
	}

	return nil
}

func (fl FixtureLoader) loadFixtureFromData(data data, opt option) error {
	builder := squirrel.Insert(opt.table).Columns(data.columns...)

	if opt.ignore {
		builder = builder.Options("IGNORE")
	}

	if opt.update {
		builder = buildOnDuplicate(data.columns, builder)
	}

	tx, err := fl.txmanager.TxBegin()
	if err != nil {
		return err
	}
	defer tx.TxFinish()

	if opt.delete {
		query, args, err := squirrel.Delete(opt.table).ToSql()
		if err != nil {
			tx.TxRollback()
			return err
		}
		tx.Exec(query, args...)
	}

	var query string
	var args []interface{}

	if opt.bulkInsert {
		for _, row := range data.rows {
			value := make([]interface{}, 0)
			for _, column := range data.columns {
				value = append(value, row[column])
			}
			builder = builder.Values(value...)
		}
		query, args, err = builder.ToSql()
		_, err = tx.Exec(query, args...)
	} else {
		for _, row := range data.rows {
			value := make([]interface{}, 0)
			for _, column := range data.columns {
				value = append(value, row[column])
			}
			query, args, err = builder.Values(value...).ToSql()
			_, err = tx.Exec(query, args...)
		}
	}

	if err != nil {
		tx.TxRollback()
		return err
	}

	err = tx.TxCommit()
	if err != nil {
		return err
	}

	return nil
}

func parseOption(opt map[string]interface{}) option {
	option := option{}

	if update, ok := opt["update"].(bool); ok {
		option.update = update
	}

	if ignore, ok := opt["ignore"].(bool); ok {
		option.ignore = ignore
	}

	if delete, ok := opt["delete"].(bool); ok {
		option.delete = delete
	}

	if bulkInser, ok := opt["bulk_insert"].(bool); ok {
		option.bulkInsert = bulkInser
	}

	if option.update && option.ignore {
		log.Fatalf("update and ignore are exclusive argument")
	}

	return option
}

func buildOnDuplicate(columns []string, builder squirrel.InsertBuilder) squirrel.InsertBuilder {
	values := make([]string, 0, len(columns))
	for _, column := range columns {
		values = append(values, fmt.Sprintf("%s = VALUES(%s)", column, column))
	}
	suffix := fmt.Sprintf("ON DUPLICATE KEY UPDATE %s", strings.Join(values, ", "))
	return builder.Suffix(suffix)
}
