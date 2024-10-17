package duckdb

import (
	"database/sql"
	"fmt"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/tomllt/DataGo/core"
)

type DuckdbReader struct {
	Dsn   string
	Query string
}

func NewDuckdbReader() *DuckdbReader {
	return &DuckdbReader{
		Dsn:   "test.db",
		Query: "SELECT * FROM duckdb_extensions()",
	}
}

func (r *DuckdbReader) Read() ([]core.Record, error) {
	db, err := sql.Open("duckdb", r.Dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB connection: %w", err)
	}
	defer db.Close()

	rows, err := db.Query(r.Query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	var records []core.Record
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		err := rows.Scan(valuePtrs...)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
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

		records = append(records, core.Record{Data: record})
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error during row iteration: %w", err)
	}

	return records, nil
}
