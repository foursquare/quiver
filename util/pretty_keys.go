package util

import (
	"encoding/hex"
	"strings"
)

func PrettyKeys(keys [][]byte) string {
	hexKeys := make([]string, len(keys))
	for i, key := range keys {
		hexKeys[i] = hex.EncodeToString(key)
	}
	return "\t" + strings.Join(hexKeys, "\n\t") + "\n"
}
