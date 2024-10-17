package core

import (
	"fmt"
	"github.com/tomllt/DataGo/plugin/reader/duckdb"
	"github.com/tomllt/DataGo/plugin/writer/stream"
	"sync"
)

type Reader interface {
	Read(chan<- Record)
}

type Writer interface {
	Write(<-chan Record)
}

type Record struct {
	Data interface{}
}

type Job struct {
	config *JobConfig
}

func NewJob(config *JobConfig) *Job {
	return &Job{config: config}
}

func (j *Job) Run() error {
	fmt.Println("Starting job...")

	readerConfig := j.config.Content.Reader
	writerConfig := j.config.Content.Writer
	channelConfig := j.config.Content.Channel

	reader, err := CreateReader(readerConfig)
	if err != nil {
		return fmt.Errorf("failed to create reader: %v", err)
	}

	writer, err := CreateWriter(writerConfig)
	if err != nil {
		return fmt.Errorf("failed to create writer: %v", err)
	}

	recordChan := make(chan Record, channelConfig.RecordCapacity)

	var wg sync.WaitGroup
	wg.Add(1)

	// Start the reader goroutine
	go func() {
		defer wg.Done()
		reader.Read(recordChan)
		close(recordChan)
	}()

	// Start multiple writer goroutines
	numGoroutines := j.config.Job.Setting["writerThreadNum"].(int)
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			writer.Write(recordChan)
		}()
	}

	// Wait for all goroutines to finish
	wg.Wait()

	fmt.Println("Job completed")
	return nil
}

func CreateReader(config ReaderConfig) (Reader, error) {
	switch config.Plugin {
	case "duckdb":
		return duckdb.NewDuckdbReader(config.Params), nil
	default:
		return nil, fmt.Errorf("unknown reader plugin: %s", config.Plugin)
	}
}

func CreateWriter(config WriterConfig) (Writer, error) {
	switch config.Plugin {
	case "stream":
		return stream.NewStreamWriter(config.Params), nil
	default:
		return nil, fmt.Errorf("unknown writer plugin: %s", config.Plugin)
	}
}
