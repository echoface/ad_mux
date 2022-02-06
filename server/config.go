package server

import (
	"io/ioutil"

	"gopkg.in/yaml.v3"
)

type (
	BSConfig struct {
		ServerBind string `yaml:"server_bind"`

		// RaftLogPath  string `yaml:"raft_log_path"`
		RaftBindAddr string `yaml:"raft_bind_addr"`

		DocStoreVolume string `yaml:"doc_store_volume"`
	}
)

func NewConfig(file string) (*BSConfig, error) {
	content, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}
	var config BSConfig
	err = yaml.Unmarshal(content, &config)
	return &config, err
}
