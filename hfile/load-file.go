// Copyright (C) 2015 Foursquare Labs Inc.

package hfile

import (
	"fmt"
	"io"
	"log"
	"os"
	"reflect"
	"unsafe"

	"github.com/edsrzf/mmap-go"
)

/*
#include<stdlib.h>
*/
import "C"

/*
Allocate a []byte outside the control of the garbage collector.

This memory will never be freed -- don't use this unless you are sure that is what you want.

Putting gigs and gigs of static, long-lived data on the gc's managed heap has the potential to
throw off any heuristics which to use the total size of the heap (e.g. maintain some % free space).
*/
func offheapMalloc(size int) []byte {
	hdr := reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(C.malloc(C.size_t(size)))),
		Len:  size,
		Cap:  size,
	}
	return *(*[]byte)(unsafe.Pointer(&hdr))
}

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
		defer f.Close()

		log.Printf("[Reader.NewReader] Reading in %s (%.02fmb)...\n", name, sizeMb)
		data := offheapMalloc(int(fi.Size()))
		if _, err := io.ReadFull(f, data); err != nil {
			log.Printf("[Reader.NewReader] Error reading in %s: %s\n", name, err.Error())
			return nil, err
		}
		log.Printf("[Reader.NewReader] Loaded %s.\n", name)
		return data, nil

	}
	return mapped, nil
}
