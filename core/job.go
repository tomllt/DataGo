package core

import (
	"fmt"
	"math"
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
	reader Reader
	writer Writer
}

func NewJob(config *JobConfig, reader Reader, writer Writer) *Job {
	return &Job{
		config: config,
		reader: reader,
		writer: writer,
	}
}

func (j *Job) Run() error {
	fmt.Println("Starting job...")

	channelConfig := j.config.Content.Channel

	recordChan := make(chan Record, channelConfig.RecordCapacity)

	var wg sync.WaitGroup
	wg.Add(1)

	// Start the reader goroutine
	go func() {
		defer wg.Done()
		j.reader.Read(recordChan)
		close(recordChan)
	}()

	// Start multiple writer goroutines
	numGoroutines := int(math.Floor(j.config.Job.Setting["writerThreadNum"].(float64)))
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			j.writer.Write(recordChan)
		}()
	}

	// Wait for all goroutines to finish
	wg.Wait()

	fmt.Println("Job completed")
	return nil
}
