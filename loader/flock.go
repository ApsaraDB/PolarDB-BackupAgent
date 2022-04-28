package main

import (
	"errors"
	"github.com/gofrs/flock"
)

var lockedFiles map[string]*flock.Flock = make(map[string]*flock.Flock)

// LockFile lock the specified file, return true if locked ok, otherwise false
func LockFile(filePath string) (bool, error) {
	fileLock := flock.New(filePath)

	ok, err := fileLock.TryLock()
	if ok {
		lockedFiles[filePath] = fileLock
	}
	return ok, err
}

// UnlockFile unlock the specified file, return err if unlock failed
func UnlockFile(filePath string) error {
	fileLock, ok := lockedFiles[filePath]
	if !ok {
		return errors.New("no lock found")
	}
	if fileLock.Locked() {
		return fileLock.Unlock()
	}
	return nil
}
