package loader

import (
	"reflect"
	"testing"
)

func TestGetDataFromCSV(t *testing.T) {
	fx := FixtureLoader{}
	columns := []string{"id", "name"}
	rows := []map[string]string{map[string]string{"id": "1", "name": "エクスカリバー"}, map[string]string{"id": "2", "name": "村正"}}

	t.Run("load csv", func(t *testing.T) {
		file := "_data/item.csv"
		data, err := fx.getDataFromCSV(file, "csv")
		if err != nil {
			t.Fatalf("[error] get data from csv: %v", err)
		}

		if !reflect.DeepEqual(data.columns, columns) {
			t.Fatalf("[error] get data from csv: expect: %v but %v", columns, data.columns)
		}

		if !reflect.DeepEqual(data.rows, rows) {
			t.Fatalf("[error] get data from csv: expect: %v but %v", data.rows, rows)
		}
	})

	t.Run("load empty csv", func(t *testing.T) {
		file := "_data/zero.csv"
		data, err := fx.getDataFromCSV(file, "csv")
		if err != nil {
			t.Fatalf("[error] get data from csv: %v", err)
		}

		if !reflect.DeepEqual(data.columns, columns) {
			t.Fatalf("[error] get data from csv: expect: %v but %v", columns, data.columns)
		}

		if len(data.rows) != 0 {
			t.Fatalf("[error] load data from csv: %v", data.rows)
		}
	})

	t.Run("load tsv", func(t *testing.T) {
		file := "_data/item.tsv"
		data, err := fx.getDataFromCSV(file, "tsv")
		if err != nil {
			t.Fatalf("[error] get data from csv: %v", err)
		}

		if !reflect.DeepEqual(data.columns, columns) {
			t.Fatalf("[error] get data from csv: expect: %v but %v", columns, data.columns)
		}

		if !reflect.DeepEqual(data.rows, rows) {
			t.Fatalf("[error] get data from csv: expect: %v but %v", data.rows, rows)
		}
	})
}
