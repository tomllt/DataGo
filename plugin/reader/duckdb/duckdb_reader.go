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

	var records []core.Record
	for rows.Next() {
		var data interface{}
		err := rows.Scan(&data)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		records = append(records, core.Record{Data: data})
	}

	return records, nil
}
