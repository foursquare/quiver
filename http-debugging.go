package main

import (
	"encoding/hex"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/foursquare/gohfile"
)

type DebugHandler struct {
	*hfile.CollectionSet
}

func (h *DebugHandler) ServeHTTP(out http.ResponseWriter, req *http.Request) {
	parts := strings.Split(req.RequestURI[1:], "/")
	if len(parts) < 1 || len(parts[0]) < 1 {
		for _, i := range h.Collections {
			fmt.Fprintf(out, "%s:\t %s (mem: %v)\n", i.Config.Name, i.Config.Path, i.Config.Mlock)
		}
	} else {
		col := parts[0]
		reader, err := h.ReaderFor(col)
		if err != nil {
			http.Error(out, err.Error(), 500)
		} else {
			scanner := hfile.NewScanner(reader)
			if len(parts) > 1 {
				key := make([]byte, len(parts[1])/2)
				n, err := hex.Decode(key, []byte(parts[1]))
				if err != nil {
					http.Error(out, err.Error(), 401)
				} else {
					log.Print("[Debug] key: %v", key)
					values, err := scanner.GetAll(key)
					if err != nil {
						http.Error(out, err.Error(), 500)
					}
					if len(values) > 0 {
						for _, value := range values {
							fmt.Fprintf(out, "%s %v\n", value, value)
						}
					} else {
						http.Error(out, fmt.Sprintf("Not found: %s/%v (%db)", parts[1], key, n), 404)
					}
				}
			} else {
				reader.PrintDebugInfo(out)
			}
		}
	}
}
