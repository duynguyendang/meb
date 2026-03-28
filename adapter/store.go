package adapter

import (
	"strings"

	"github.com/dgraph-io/badger/v4"
	"github.com/duynguyendang/meb/keys"
)

type FactSource interface {
	Search(predicate string, args []any) Iterator
}

type MebAdapter struct {
	db   *badger.DB
	dict Dictionary
}

func NewMebAdapter(db *badger.DB, dict Dictionary) *MebAdapter {
	return &MebAdapter{
		db:   db,
		dict: dict,
	}
}

func (a *MebAdapter) Search(predicate string, args []any) Iterator {
	if predicate != "triples" {
		return &EmptyIterator{}
	}

	if len(args) != 3 {
		return &EmptyIterator{}
	}

	var sBound, pBound, oBound bool
	var sVal, pVal, oVal string

	if args[0] != nil {
		sBound = true
		sVal = strings.Trim(args[0].(string), "\"")
	}
	if args[1] != nil {
		pBound = true
		pVal = strings.Trim(args[1].(string), "\"")
	}
	if args[2] != nil {
		oBound = true
		oVal = strings.Trim(args[2].(string), "\"")
	}

	if !sBound && !pBound && !oBound {
		return &EmptyIterator{}
	}

	var prefix []byte

	if sBound {
		sID, err := a.dict.GetID(sVal)
		if err != nil {
			return &EmptyIterator{}
		}

		var pID uint64
		if pBound {
			pID, err = a.dict.GetID(pVal)
			if err != nil {
				return &EmptyIterator{}
			}
		}

		if pID != 0 {
			prefix = keys.EncodeTripleSPOPrefix(sID, pID, 0)
		} else {
			prefix = keys.EncodeTripleSPOPrefix(sID, 0, 0)
		}

	} else if oBound {
		oID, err := a.dict.GetID(oVal)
		if err != nil {
			return &EmptyIterator{}
		}

		var pID uint64
		if pBound {
			pID, err = a.dict.GetID(pVal)
			if err != nil {
				return &EmptyIterator{}
			}
		}

		if pID != 0 {
			prefix = keys.EncodeTripleOPSPrefix(oID, pID, 0)
		} else {
			prefix = keys.EncodeTripleOPSPrefix(oID, 0, 0)
		}

	} else {
		return &EmptyIterator{}
	}

	txn := a.db.NewTransaction(false)
	return NewBadgerIterator(txn, prefix, a.dict)
}
