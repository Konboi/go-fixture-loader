package loader

import (
	"reflect"
	"sort"
	"testing"
)

func TestGetDataFromJSON(t *testing.T) {
	fx := FixtureLoader{}
	columns := []string{"id", "name"}

	rows := []map[string]string{map[string]string{"id": "5", "name": "グラディウス"}, map[string]string{"id": "6", "name": "木刀"}}

	t.Run("load json", func(t *testing.T) {
		file := "_data/item.json"

		data, err := fx.getDataFromJSON(file)
		if err != nil {
			t.Fatalf("[error] get data from json: %v", err)
		}

		sort.Strings(data.columns)
		sort.Strings(columns)
		if !reflect.DeepEqual(data.columns, columns) {
			t.Fatalf("[error] get data from csv: expect: %v but %v", columns, data.columns)
		}

		if !reflect.DeepEqual(data.rows, rows) {
			t.Fatalf("[error] get data from csv: expect: %v but %v", data.rows, rows)
		}
	})
}
