package utils

import "hash/fnv"

func HashString(s string, n int) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	hashValue := h.Sum32()
	return int(hashValue%uint32(n)) + 1
}
