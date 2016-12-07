package loader

import (
	"encoding/csv"
	"io"
	"os"
)

func (fx FixtureLoader) getDataFromCSV(file, format string) (data, error) {
	f, err := os.Open(file)
	if err != nil {
		return data{}, err
	}
	defer f.Close()

	reader := csv.NewReader(f)
	if format == "tsv" {
		reader.Comma = '\t'
	}

	columns, err := reader.Read()
	if err != nil {
		return data{}, err
	}

	data := data{columns: columns}
	for {
		row, err := reader.Read()

		if err != nil {
			if err == io.EOF {
				break
			}
			return data, err
		}

		rows := make(map[string]string, len(row))
		for i, value := range row {
			rows[data.columns[i]] = value
		}

		data.rows = append(data.rows, rows)
	}

	return data, nil
}
