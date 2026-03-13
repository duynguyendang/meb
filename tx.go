package meb

import (
	"sync"

	"github.com/dgraph-io/badger/v4"
)

type TxPool struct {
	db          *badger.DB
	readPool    chan *badger.Txn
	writePool   chan *badger.Txn
	poolSize    int
	initialized bool
	mu          sync.RWMutex
}

func NewTxPool(db *badger.DB, poolSize int) *TxPool {
	return &TxPool{
		db:        db,
		poolSize:  poolSize,
		readPool:  make(chan *badger.Txn, poolSize),
		writePool: make(chan *badger.Txn, poolSize),
	}
}

func (p *TxPool) Init() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.initialized {
		return
	}

	for i := 0; i < p.poolSize; i++ {
		p.readPool <- p.db.NewTransaction(false)
		p.writePool <- p.db.NewTransaction(true)
	}
	p.initialized = true
}

func (p *TxPool) GetRead() *badger.Txn {
	select {
	case txn := <-p.readPool:
		return txn
	default:
		return p.db.NewTransaction(false)
	}
}

func (p *TxPool) PutRead(txn *badger.Txn) {
	select {
	case p.readPool <- txn:
	default:
	}
}

func (p *TxPool) GetWrite() *badger.Txn {
	select {
	case txn := <-p.writePool:
		return txn
	default:
		return p.db.NewTransaction(true)
	}
}

func (p *TxPool) PutWrite(txn *badger.Txn) {
	select {
	case p.writePool <- txn:
	default:
	}
}

func (p *TxPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.initialized {
		return
	}

	close(p.readPool)
	close(p.writePool)
	for txn := range p.readPool {
		txn.Discard()
	}
	for txn := range p.writePool {
		txn.Discard()
	}
	p.initialized = false
}

// withReadTxn executes a read transaction.
// Creates a fresh transaction each time to avoid pooling issues.
func (m *MEBStore) withReadTxn(fn func(*badger.Txn) error) error {
	txn := m.db.NewTransaction(false)
	if err := fn(txn); err != nil {
		txn.Discard()
		return err
	}
	txn.Discard()
	return nil
}

// withWriteTxn executes a function within a write transaction.
// Creates a fresh transaction each time to avoid pooling issues.
func (m *MEBStore) withWriteTxn(fn func(*badger.Txn) error) error {
	txn := m.db.NewTransaction(true)
	if err := fn(txn); err != nil {
		txn.Discard()
		return err
	}
	return txn.Commit()
}
