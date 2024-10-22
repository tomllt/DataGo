package core

import "fmt"

type SimpleAlert struct{}

func (a *SimpleAlert) Trigger(message string) error {
	fmt.Printf("Alert triggered: %s\n", message)
	return nil
}