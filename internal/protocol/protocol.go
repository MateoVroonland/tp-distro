package protocol

import (
	"bytes"
	"encoding/csv"
)

type Protocol interface {
	Deserialize(data []string) error
	GetRawData() []string
}

func Serialize(v Protocol) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	writer := csv.NewWriter(buf)

	rawData := v.GetRawData()

	writer.Write(rawData)
	writer.Flush()

	return buf.Bytes(), nil
}
