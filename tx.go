package meb

import (
	"github.com/dgraph-io/badger/v4"
)

func (m *MEBStore) withReadTxn(fn func(*badger.Txn) error) error {
	txn := m.db.NewTransaction(false)
	defer txn.Discard()
	return fn(txn)
}

func (m *MEBStore) withWriteTxn(fn func(*badger.Txn) error) error {
	txn := m.db.NewTransaction(true)
	if err := fn(txn); err != nil {
		txn.Discard()
		return err
	}
	return txn.Commit()
}
