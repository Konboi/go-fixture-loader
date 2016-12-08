package loader

import (
	"reflect"
	"sort"
	"testing"
)

func TestGetDataFromYAML(t *testing.T) {
	fx := FixtureLoader{}
	columns := []string{"id", "name"}
	rows := []map[string]string{map[string]string{"id": "4", "name": "ホーリーランス"}, map[string]string{"id": "3", "name": "ウィザードロッド"}}

	t.Run("load yaml", func(t *testing.T) {
		file := "_data/item.yaml"
		data, err := fx.getDataFromYAML(file)
		if err != nil {
			t.Fatalf("[error] get data from yaml: %v", err)
		}

		sort.Strings(data.columns)
		sort.Strings(columns)
		if !reflect.DeepEqual(data.columns, columns) {
			t.Fatalf("[error] get data from yaml: expect: %v but %v", columns, data.columns)
		}

		if !reflect.DeepEqual(data.rows, rows) {
			t.Fatalf("[error] get data from yaml: expect: %v but %v", data.rows, rows)
		}
	})
}
