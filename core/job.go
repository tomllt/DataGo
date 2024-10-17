package core

import (
	"fmt"
)

type Reader interface {
	Read() ([]Record, error)
}

type Writer interface {
	Write([]Record) error
}

type Record struct {
	Data interface{}
}

type JobConfig struct {
	Reader Reader
	Writer Writer
}

type Job struct {
	config *JobConfig
}

func NewJob(config *JobConfig) *Job {
	return &Job{config: config}
}

func (j *Job) Run() error {
	fmt.Println("Starting job...")

	records, err := j.config.Reader.Read()
	if err != nil {
		return fmt.Errorf("read failed: %w", err)
	}

	err = j.config.Writer.Write(records)
	if err != nil {
		return fmt.Errorf("write failed: %w", err)
	}

	fmt.Println("Job completed")
	return nil
}