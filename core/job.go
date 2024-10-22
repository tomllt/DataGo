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

type Monitor interface {
    Start() error
    Stop() error
    ReportMetrics() Metrics
}

type Alert interface {
    Trigger(message string) error
}

type Metrics struct {
    JobID      string
    Status     string
    Progress   float64
    Errors     []string
}

type Job struct {
	config  *JobConfig
	reader  Reader
	writer  Writer
	monitor Monitor
	alert   Alert
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

	// Start the monitoring
	if err := j.monitor.Start(); err != nil {
		fmt.Printf("Failed to start monitor: %v\n", err)
		return err
	}
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

		j.monitor.ReportMetrics() // Reporting metrics after completion
		fmt.Println("Job completed")
		if err := j.monitor.Stop(); err != nil {
			fmt.Printf("Failed to stop monitor: %v\n", err)
		}
	return nil
}
