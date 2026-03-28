package meb

import "github.com/dgraph-io/badger/v4"

func GetMEBOptions(isReadOnly bool) badger.Options {
	opts := badger.DefaultOptions("")

	opts.ReadOnly = isReadOnly
	opts.Logger = nil

	if isReadOnly {
		opts.IndexCacheSize = 256 << 20
		opts.BlockCacheSize = 64 << 20
	} else {
		opts.IndexCacheSize = 2 << 30
		opts.BlockCacheSize = 1 << 30
		opts.NumCompactors = 4
		opts.CompactL0OnClose = true
	}

	return opts
}
