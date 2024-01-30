package redis_autopipeline

import (
	"bytes"
	"crypto/sha256"
	"fmt"
)

const hashDelimiter = ","

// hashStringSlice returns unique hash for the redisOperation with the operation arguments
func hashStringSlice(operation operationPrefix, args []string) string {
	var buffer bytes.Buffer
	buffer.WriteByte(byte(operation))
	for _, s := range args {
		buffer.WriteString(s)
		buffer.WriteString(hashDelimiter)
	}
	hash := sha256.Sum256([]byte(buffer.String()))
	return fmt.Sprintf("%x", hash)
}
