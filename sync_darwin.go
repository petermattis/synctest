// +build darwin

package main

import (
	"os"
	"syscall"
)

func fdatasync(f *os.File) error {
	// return syscall.Fsync(int(f.Fd()))
	_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, f.Fd(),
		uintptr(syscall.F_FULLFSYNC), uintptr(0))
	if errno == 0 {
		return nil
	}
	return errno
}
