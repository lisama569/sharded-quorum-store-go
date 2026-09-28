package store

import (
	"crypto/sha256"
	"hash"
)

func newSHA256() hash.Hash {
	return sha256.New()
}
