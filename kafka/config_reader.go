package kafka

import (
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

type FileReader struct {
	fileName string
}

func NewfileReader(file string) FileReader {
	return FileReader{file}
}

func (f FileReader) ReadConf() (*Config, error) {
	buf, err := ioutil.ReadFile(f.fileName)
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
