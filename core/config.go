package core

type Content struct {
	Reader  ReaderConfig  `json:"reader"`
	Writer  WriterConfig  `json:"writer"`
	Channel ChannelConfig `json:"channel"`
}

type ReaderConfig struct {
	Name   string                 `json:"name"`
	Plugin string                 `json:"plugin"`
	Params map[string]interface{} `json:"params"`
}

type WriterConfig struct {
	Name   string                 `json:"name"`
	Plugin string                 `json:"plugin"`
	Params map[string]interface{} `json:"params"`
}

type ChannelConfig struct {
	ChannelClass string `json:"channelClass"`
	BytesCapacity int64 `json:"byteCapacity"`
	RecordCapacity int64 `json:"recordCapacity"`
}

type JobConfig struct {
	Job      JobSettings `json:"job"`
	Content  Content     `json:"content"`
	Monitor  string      `json:"monitor"` // Monitor type configuration
	Alert    string      `json:"alert"`   // Alert type configuration
}

type JobSettings struct {
	Setting map[string]interface{} `json:"setting"`
}