// +build linux

package main

import (
	"os"
	"syscall"
)

func fdatasync(f *os.File) error {
	return syscall.Fdatasync(int(f.Fd()))
}
