package loader

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
)

func (fx FixtureLoader) getDataFromJSON(file string) (data, error) {
	f, err := ioutil.ReadFile(file)
	if err != nil {
		return data{}, err
	}

	var jsonData interface{}
	json.Unmarshal(f, &jsonData)

	if jsonData == nil {
		return data{}, fmt.Errorf("[error] please check file data format")
	}
	rows := make([]map[string]string, 0)

	for _, d := range jsonData.([]interface{}) {
		if _, ok := d.(map[string]interface{}); !ok {
			return data{}, fmt.Errorf("[error] please check json data format not. format isn't map[string]interface{} ")
		}
		row := dataToMapString(d.(map[string]interface{}))
		rows = append(rows, row)
	}

	if len(rows) < 1 {
		return data{}, fmt.Errorf("[error] %s is data empty", file)
	}
	columns := make([]string, 0)
	for key, _ := range rows[0] {
		columns = append(columns, key)
	}

	data := data{
		columns: columns,
		rows:    rows,
	}

	return data, nil
}

func dataToMapString(d map[string]interface{}) map[string]string {
	row := make(map[string]string, len(d))
	for key, value := range d {
		switch t := value.(type) {
		case string:
			row[key] = value.(string)
		case float64:
			row[key] = strconv.Itoa(int(value.(float64)))
		default:
			log.Fatalf("[error] not define type: %v. please issue", t)
		}
	}

	return row
}
