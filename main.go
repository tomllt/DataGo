package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/tomllt/DataGo/core"
	"github.com/tomllt/DataGo/plugin/reader/duckdb"
	"github.com/tomllt/DataGo/plugin/writer/stream"
)

func main() {
	// 从文件中读取配置
	configData, err := os.ReadFile("job_config.json")
	if err != nil {
		log.Fatalf("Failed to read config file: %v", err)
	}

	// 解析配置
	var jobConfig core.JobConfig
	err = json.Unmarshal(configData, &jobConfig)
	if err != nil {
		log.Fatalf("Failed to parse config: %v", err)
	}

	// 创建 reader 和 writer
	reader, err := CreateReader(jobConfig.Content.Reader)
	if err != nil {
		log.Fatalf("Failed to create reader: %v", err)
	}

	writer, err := CreateWriter(jobConfig.Content.Writer)
	if err != nil {
		log.Fatalf("Failed to create writer: %v", err)
	}

	// 创建并运行任务
	job := core.NewJob(&jobConfig, reader, writer)
	err = job.Run()
	if err != nil {
		log.Fatalf("Job failed: %v", err)
	}

	fmt.Println("Job completed successfully")
}

func CreateReader(config core.ReaderConfig) (core.Reader, error) {
	switch config.Plugin {
	case "duckdb":
		return duckdb.NewDuckdbReader(config.Params), nil
	default:
		return nil, fmt.Errorf("unknown reader plugin: %s", config.Plugin)
	}
}

func CreateWriter(config core.WriterConfig) (core.Writer, error) {
	switch config.Plugin {
	case "stream":
		return stream.NewStreamWriter(config.Params), nil
	default:
		return nil, fmt.Errorf("unknown writer plugin: %s", config.Plugin)
	}
}
