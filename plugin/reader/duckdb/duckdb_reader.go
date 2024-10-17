package duckdb

import (
	"database/sql"
	"fmt"
	"github.com/tomllt/DataGo/core"

	_ "github.com/marcboeker/go-duckdb"
)

type DuckdbReader struct {
	Dsn   string
	Query string
}

func NewDuckdbReader(params map[string]interface{}) *DuckdbReader {
	dsn, _ := params["dsn"].(string)
	query, _ := params["query"].(string)
	return &DuckdbReader{
		Dsn:   dsn,
		Query: query,
	}
}

func (r *DuckdbReader) Read(recordChan chan<- core.Record) {
	db, err := sql.Open("duckdb", r.Dsn)
	if err != nil {
		fmt.Printf("Failed to open DuckDB connection: %v\n", err)
		return
	}
	defer db.Close()

	rows, err := db.Query(r.Query)
	if err != nil {
		fmt.Printf("Failed to execute query: %v\n", err)
		return
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		fmt.Printf("Failed to get columns: %v\n", err)
		return
	}

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		err := rows.Scan(valuePtrs...)
		if err != nil {
			fmt.Printf("Failed to scan row: %v\n", err)
			continue
		}

		record := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				record[col] = string(b)
			} else {
				record[col] = val
			}
		}

		recordChan <- core.Record{Data: record}
	}

	if err = rows.Err(); err != nil {
		fmt.Printf("Error during row iteration: %v\n", err)
	}
}
