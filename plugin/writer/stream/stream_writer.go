package stream

import (
	"encoding/json"
	"fmt"

	"github.com/tomllt/DataGo/core"
)

type StreamWriter struct{}

func NewStreamWriter() *StreamWriter {
	return &StreamWriter{}
}

func (w *StreamWriter) Write(recordChan <-chan core.Record) {
	for record := range recordChan {
		jsonData, err := json.Marshal(record.Data)
		if err != nil {
			fmt.Printf("Failed to marshal record: %v\n", err)
			continue
		}
		fmt.Println(string(jsonData))
	}
}
