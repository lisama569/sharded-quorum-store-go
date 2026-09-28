package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
)

// Client is a thin façade over a Cluster that supplies idempotency keys
// and default options, and normalizes results.
type Client struct {
	cluster *Cluster
	defW    WriteOptions
	defR    ReadOptions
}

// NewClient wraps a cluster with default consistency Quorum.
func NewClient(c *Cluster) *Client {
	return &Client{
		cluster: c,
		defW:    WriteOptions{Consistency: Quorum},
		defR:    ReadOptions{Consistency: Quorum},
	}
}

// WithDefaults returns a client with adjusted default options.
func (c *Client) WithDefaults(w WriteOptions, r ReadOptions) *Client {
	cp := *c
	cp.defW, cp.defR = w, r
	return &cp
}

// Put writes a key. txnID "" autogenerates one; pass a stable txnID to retry.
func (c *Client) Put(ctx context.Context, key Key, val Value, txnID string) (WriteResult, error) {
	return c.cluster.Put(ctx, key, val, txnID, c.defW)
}

// Delete removes a key.
func (c *Client) Delete(ctx context.Context, key Key, txnID string) (WriteResult, error) {
	return c.cluster.Delete(ctx, key, txnID, c.defW)
}

// Get fetches a key.
func (c *Client) Get(ctx context.Context, key Key) (ReadResult, error) {
	return c.cluster.Get(ctx, key, c.defR)
}

// NewTxnID returns a random idempotency key.
func NewTxnID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
