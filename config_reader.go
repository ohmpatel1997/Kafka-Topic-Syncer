package main

import (
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

type Config struct {
	BottstrapServer string  `yaml:"bootstrapServers"`
	Replication     int64   `yaml:"replication"`
	Partitions      int64   `yaml:"partitions"`
	Topics          []topic `yaml:"topics"`
}

type topic struct {
	Name        string `yaml:"name"`
	RetentionMs int64  `yaml:"retentionMs"`
	Compression string `yaml:"compression"`
}

func readConf(filename string) (*Config, error) {
	buf, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var config = new(Config)
	err = yaml.Unmarshal(buf, config)
	if err != nil {
		return nil, err
	}

	return config, nil
}
