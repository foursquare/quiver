package hfile

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/edsrzf/mmap-go"
)

func loadFile(name, path string, method LoadMethod) ([]byte, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)

	if err != nil {
		return nil, fmt.Errorf("[Reader] Error opening file (%s): %v", path, err)
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	sizeMb := float64(fi.Size()) / (1024.0 * 1024.0)

	var mapped mmap.MMap
	if method != CopiedToMem {
		mapped, err = mmap.Map(f, mmap.RDONLY, 0)

		if err != nil {
			log.Printf("[Reader.NewReader] Error mapping %s: %s\n", name, err.Error())
			return nil, err
		}
	}

	switch method {

	case OnDisk:
		log.Printf("[Reader.NewReader] Serving %s from disk...\n", name)

	case MemlockFile:
		log.Printf("[Reader.NewReader] Locking %s (%.02fmb)...\n", name, sizeMb)
		if err = mapped.Lock(); err != nil {
			log.Printf("[Reader.NewReader] Error locking %s: %s\n", name, err.Error())
			return nil, err
		}
		log.Printf("[Reader.NewReader] Locked %s.\n", name)

	case CopiedToMem:
		log.Printf("[Reader.NewReader] Reading in %s (%.02fmb)...\n", name, sizeMb)
		data, err := ioutil.ReadFile(path)
		if err != nil {
			log.Printf("[Reader.NewReader] Error reading in %s: %s\n", name, err.Error())
			return nil, err
		}
		log.Printf("[Reader.NewReader] Loaded %s.\n", name)
		return data, nil

	}
	return mapped, nil
}
