package utils

import (
	"bytes"
	"encoding/csv"
)

func EncodeArrayToCsv(arr []string) string {
	buf := bytes.NewBuffer(nil)
	writer := csv.NewWriter(buf)

	writer.Write(arr)
	writer.Flush()

	return buf.String()
}
