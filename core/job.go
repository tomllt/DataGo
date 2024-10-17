package core

import (
	"fmt"
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
	// Factory method to create reader based on config
	// This is a placeholder and should be implemented based on your available readers
	return nil, fmt.Errorf("reader creation not implemented")
}

func CreateWriter(config WriterConfig) (Writer, error) {
	// Factory method to create writer based on config
	// This is a placeholder and should be implemented based on your available writers
	return nil, fmt.Errorf("writer creation not implemented")
}