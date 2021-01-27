package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafKa interface {
	UpdateConfiguration(*Config) error
}

type kafkaClient struct {
	*kafka.AdminClient
}

func NewKafkaClient(bootstrapHost string) (KafKa, error) {
	var kc *kafkaClient
	cm := kafka.ConfigMap{
		"bootstrap.servers": bootstrapHost}
	ac, err := kafka.NewAdminClient(&cm)
	if err != nil {
		return kc, err
	}

	return &kafkaClient{
		ac,
	}, nil
}

func (kc kafkaClient) UpdateConfiguration(configs *Config) error {

	// Get some metadata
	var md *kafka.Metadata
	var err error
	if md, err = kc.AdminClient.GetMetadata(nil, true, int(5*time.Second)); err != nil {
		return err
	}

	topicsAlreadyExist := make(map[string]bool)
	for _, t := range md.Topics {
		topicsAlreadyExist[t.Topic] = true
	}

	errChan := make(chan error, len(configs.Topics))
	updateTopicChan := make(chan topic)
	createTopicChan := make(chan topic)

	go kc.updateTopics(updateTopicChan, configs.Replication, errChan)
	go kc.createTopics(createTopicChan, configs.Replication, configs.Partitions, errChan)

	for _, topic := range configs.Topics {
		if ok := topicsAlreadyExist[topic.Name]; ok { //if exist, push to update chan
			updateTopicChan <- topic
		} else { //else, push to create chan
			createTopicChan <- topic
		}
	}

	close(updateTopicChan)
	close(createTopicChan)

	updateTopicErr := <-errChan
	createTopicErr := <-errChan

	if updateTopicChan != nil {
		return updateTopicErr
	}
	return createTopicErr
}

func (kc kafkaClient) updateTopics(topics <-chan topic, repli replication, errChan chan<- error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var configResources []kafka.ConfigResource

	for t := range topics {
		configResources = append(configResources,
			kafka.ConfigResource{
				Type: kafka.ResourceTopic,
				Name: t.Name,
				Config: []kafka.ConfigEntry{
					kafka.ConfigEntry{
						Name:      t.Compression.GetKeyString(),
						Value:     t.Compression.GetValue(),
						Operation: kafka.AlterOperationSet,
					},

					kafka.ConfigEntry{
						Name:      t.RetentionMs.GetKeyString(),
						Value:     fmt.Sprintf("%d", t.RetentionMs.GetValue()),
						Operation: kafka.AlterOperationSet,
					},

					kafka.ConfigEntry{
						Name:      repli.GetKeyString(),
						Value:     fmt.Sprintf("%d", repli.GetValue()),
						Operation: kafka.AlterOperationSet,
					},
				},
			})
	}

	_, err := kc.AdminClient.AlterConfigs(ctx, configResources)
	errChan <- err
}

func (kc kafkaClient) createTopics(topics <-chan topic, repli replication, parti partition, errChan chan<- error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var topicResources []kafka.TopicSpecification

	for t := range topics {
		topicResources = append(topicResources,
			kafka.TopicSpecification{
				Topic:             t.Name,
				NumPartitions:     int(parti.GetValue()),
				ReplicationFactor: int(repli.GetValue()),
				Config: map[string]string{
					t.Compression.GetKeyString(): t.Compression.GetValue(),
					t.RetentionMs.GetKeyString(): fmt.Sprintf("%d", t.RetentionMs.GetValue()),
				},
			})
	}

	_, err := kc.AdminClient.CreateTopics(ctx, topicResources)
	errChan <- err
}
