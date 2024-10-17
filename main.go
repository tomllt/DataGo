package main

import (
	"fmt"
	"log"

	"github.com/tomllt/DataGo/core"
	"github.com/tomllt/DataGo/plugin/reader/duckdb"
	"github.com/tomllt/DataGo/plugin/writer/stream"
)

func main() {
	// 创建一个新的任务配置
	conf := &core.JobConfig{
		Reader: duckdb.NewDuckdbReader(),
		Writer: stream.NewStreamWriter(),
	}

	// 创建并运行任务
	job := core.NewJob(conf)
	err := job.Run()
	if err != nil {
		log.Fatalf("Job failed: %v", err)
	}

	fmt.Println("Job completed successfully")
}
