package store

import "crypto/sha256"

// hasher 是哈希器的最小抽象，便于替换实现。
type hasher interface {
	Write(p []byte) (int, error)
	Sum(b []byte) []byte
}

func newSHA256() hasher { return sha256.New() }
