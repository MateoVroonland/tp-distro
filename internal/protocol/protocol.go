package protocol

import "github.com/MateoVroonland/tp-distro/internal/utils"

type Protocol interface {
	Deserialize(data []string) error
	GetRawData() []string
}

func Serialize(v Protocol) ([]byte, error) {
	stringLine := utils.EncodeArrayToCsv(v.GetRawData())

	return []byte(stringLine), nil
}
