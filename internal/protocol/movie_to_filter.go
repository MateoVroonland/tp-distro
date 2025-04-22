package protocol

type MovieToFilter interface {
	Deserialize(data []string) error
	GetRawData() []string
	PassesFilter() bool
	GetRoutingKey() string
}
