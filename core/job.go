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

type JobConfig struct {
	Reader        Reader
	Writer        Writer
	BufferSize    int
	NumGoroutines int
}

type Job struct {
	config *JobConfig
}

func NewJob(config *JobConfig) *Job {
	if config.BufferSize == 0 {
		config.BufferSize = 1000
	}
	if config.NumGoroutines == 0 {
		config.NumGoroutines = 5
	}
	return &Job{config: config}
}

func (j *Job) Run() error {
	fmt.Println("Starting job...")

	recordChan := make(chan Record, j.config.BufferSize)

	var wg sync.WaitGroup
	wg.Add(1)

	// Start the reader goroutine
	go func() {
		defer wg.Done()
		j.config.Reader.Read(recordChan)
		close(recordChan)
	}()

	// Start multiple writer goroutines
	for i := 0; i < j.config.NumGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			j.config.Writer.Write(recordChan)
		}()
	}

	// Wait for all goroutines to finish
	wg.Wait()

	fmt.Println("Job completed")
	return nil
}