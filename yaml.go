package loader

import (
	"fmt"
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

func (fx FixtureLoader) getDataFromYAML(file string) (data, error) {
	f, err := ioutil.ReadFile(file)
	if err != nil {
		return data{}, err
	}

	var yamlData interface{}
	yaml.Unmarshal(f, &yamlData)

	rows := make([]map[string]string, 0)
	for _, d := range yamlData.([]interface{}) {
		row := interfaceInterfaceToMapString(d.(map[interface{}]interface{}))
		rows = append(rows, row)
	}

	if len(rows) < 1 {
		return data{}, fmt.Errorf("[error] %s is data empty", file)
	}

	columns := make([]string, 0)
	for key := range rows[0] {
		columns = append(columns, key)
	}

	data := data{
		columns: columns,
		rows:    rows,
	}

	return data, nil
}

// onply support testfixtures yaml type refs: https://github.com/go-testfixtures/testfixtures#usage
func interfaceInterfaceToMapString(d map[interface{}]interface{}) map[string]string {
	row := make(map[string]string, len(d))
	for key, value := range d {
		row[fmt.Sprint(key)] = fmt.Sprint(value)
	}

	return row
}
