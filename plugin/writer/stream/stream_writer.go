package stream

import (
	"encoding/json"
	"fmt"

	"github.com/yourusername/DataGo/core"
)

type StreamWriter struct{}

func NewStreamWriter() *StreamWriter {
	return &StreamWriter{}
}

func (w *StreamWriter) Write(records []core.Record) error {
	for _, record := range records {
		jsonData, err := json.Marshal(record.Data)
		if err != nil {
			return fmt.Errorf("failed to marshal record: %w", err)
		}
		fmt.Println(string(jsonData))
	}
	return nil
}