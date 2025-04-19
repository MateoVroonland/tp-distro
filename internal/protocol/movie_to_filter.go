package protocol

type MovieToFilter interface {
	Deserialize(data []string) error
	GetRawData() []string
	Is2000s() bool
}
