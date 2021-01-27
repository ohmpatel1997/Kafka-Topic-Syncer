package main

import (
	"fmt"
	"os"

	"github.com/ohmpatel1997/Kafka-Topic-Syncer/kafka"
)

func main() {
	args := os.Args
	if len(args) != 2 {
		panic("invalid number of arguments. only config file name should be provided")
	}

	fileName := args[1]

	fReader := kafka.NewfileReader(fileName)
	conf, err := fReader.ReadConf()
	if err != nil {
		panic(fmt.Sprintf("error reading config file: %s", err.Error()))
	}

	client, err := kafka.NewKafkaClient(conf.BottstrapServer)
	if err != nil {
		panic(err)
	}

	client.UpdateConfiguration(conf)
}
