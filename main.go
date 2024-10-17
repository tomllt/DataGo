package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/tomllt/DataGo/core"
)

func main() {
	// 从文件中读取配置
	configData, err := ioutil.ReadFile("job_config.json")
	if err != nil {
		log.Fatalf("Failed to read config file: %v", err)
	}

	// 解析配置
	var jobConfig core.JobConfig
	err = json.Unmarshal(configData, &jobConfig)
	if err != nil {
		log.Fatalf("Failed to parse config: %v", err)
	}

	// 创建并运行任务
	job := core.NewJob(&jobConfig)
	err = job.Run()
	if err != nil {
		log.Fatalf("Job failed: %v", err)
	}

	fmt.Println("Job completed successfully")
}
