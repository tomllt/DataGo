package core

import (
	"fmt"
	"time"
)

type SimpleMonitor struct {
	jobID    string
	started  time.Time
	progress float64
}

func (m *SimpleMonitor) Start() error {
	m.started = time.Now()
	fmt.Println("Monitoring started")
	return nil
}

func (m *SimpleMonitor) Stop() error {
	fmt.Println("Monitoring stopped")
	return nil
}

func (m *SimpleMonitor) ReportMetrics() Metrics {
	return Metrics{
		JobID:    m.jobID,
		Status:   "Completed",
		Progress: m.progress,
		Errors:   nil,
	}
}