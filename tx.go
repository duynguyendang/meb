package meb

import "github.com/dgraph-io/badger/v4"

// withReadTxn executes a function within a read transaction.
func (m *MEBStore) withReadTxn(fn func(*badger.Txn) error) error {
	txn := m.newTxn()
	defer m.releaseTxn(txn)
	return fn(txn)
}

// withWriteTxn executes a function within a write transaction.
func (m *MEBStore) withWriteTxn(fn func(*badger.Txn) error) error {
	txn := m.db.NewTransaction(true)
	defer txn.Discard()
	if err := fn(txn); err != nil {
		return err
	}
	return txn.Commit()
}
