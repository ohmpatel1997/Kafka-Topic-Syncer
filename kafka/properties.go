package kafka

type Config struct {
	BottstrapServer string      `yaml:"bootstrapServers"`
	Replication     replication `yaml:"replication"`
	Partitions      partition   `yaml:"partitions"`
	Topics          []topic     `yaml:"topics"`
}

type partition int64

func (c partition) GetKeyString() string {
	return "partitions"
}

func (c partition) GetValue() int64 {
	return int64(c)
}

type replication int64

func (c replication) GetKeyString() string {
	return "replication-factor"
}

func (c replication) GetValue() int64 {
	return int64(c)
}

type compression string

func (c compression) GetKeyString() string {
	return "compression.type"
}

func (c compression) GetValue() string {
	return string(c)
}

type retensionMs int64

func (r retensionMs) GetKeyString() string {
	return "retention.ms"
}

func (r retensionMs) GetValue() int64 {
	return int64(r)
}

type topic struct {
	Name        string      `yaml:"name"`
	RetentionMs retensionMs `yaml:"retentionMs"`
	Compression compression `yaml:"compression"`
}
