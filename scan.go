package meb

import (
	"context"
	"fmt"
	"iter"
	"regexp"
	"strconv"

	"github.com/duynguyendang/meb/keys"

	"github.com/dgraph-io/badger/v4"
)

type PredicateFilterType string

const (
	PredicateRegex    PredicateFilterType = "regex"
	PredicateRange    PredicateFilterType = "range"
	PredicateGT       PredicateFilterType = "gt"
	PredicateLT       PredicateFilterType = "lt"
	PredicateGTE      PredicateFilterType = "gte"
	PredicateLTE      PredicateFilterType = "lte"
	PredicateContains PredicateFilterType = "contains"
)

type PredicateFilter struct {
	Type  PredicateFilterType
	Value interface{}
}

type scanStrategy struct {
	prefix []byte
	index  byte
}

func selectScanStrategy(sBound, pBound, oBound bool, sID, pID, oID uint64) *scanStrategy {
	var strategy scanStrategy

	if sBound {
		strategy.prefix = keys.EncodeTripleSPOPrefix(sID, pID, oID)
		strategy.index = keys.TripleSPOPrefix
	} else if oBound {
		if pBound {
			strategy.prefix = keys.EncodeTripleOPSPrefix(oID, pID, 0)
		} else {
			strategy.prefix = keys.EncodeTripleOPSPrefix(oID, 0, 0)
		}
		strategy.index = keys.TripleOPSPrefix
	} else if pBound {
		strategy.prefix = []byte{keys.TripleSPOPrefix}
		strategy.index = keys.TripleSPOPrefix
	} else {
		strategy.prefix = []byte{keys.TripleSPOPrefix}
		strategy.index = keys.TripleSPOPrefix
	}

	return &strategy
}

func (m *MEBStore) resolveScanIDs(s, p, o string) (sID, pID, oID uint64, sBound, pBound, oBound bool, err error) {
	if s != "" {
		sID, err = m.dict.GetID(s)
		if err != nil {
			return 0, 0, 0, false, false, false, dictError("subject", s, err)
		}
		sBound = true
	}

	if p != "" {
		pID, err = m.dict.GetID(p)
		if err != nil {
			return 0, 0, 0, false, false, false, dictError("predicate", p, err)
		}
		pBound = true
	}

	if o != "" {
		oID, err = m.dict.GetID(o)
		if err != nil {
			return 0, 0, 0, false, false, false, dictError("object", o, err)
		}
		oBound = true
	}

	return sID, pID, oID, sBound, pBound, oBound, nil
}

func dictError(field, value string, err error) error {
	return fmt.Errorf("%s %q not found: %w", field, value, err)
}

type scanResult struct {
	foundSID uint64
	foundPID uint64
	foundOID uint64
	key      []byte
}

type scanOptions struct {
	ctx      context.Context
	s        string
	p        string
	o        string
	sID      uint64
	pID      uint64
	oID      uint64
	sBound   bool
	pBound   bool
	oBound   bool
	strategy *scanStrategy
	filters  []PredicateFilter
}

func evaluatePredicateFilter(objValue string, filter PredicateFilter) bool {
	switch filter.Type {
	case PredicateRegex:
		pattern, ok := filter.Value.(string)
		if !ok {
			return false
		}
		matched, err := regexp.MatchString(pattern, objValue)
		return err == nil && matched

	case PredicateContains:
		substr, ok := filter.Value.(string)
		if !ok {
			return false
		}
		return len(objValue) > 0 && len(substr) > 0 &&
			(len(objValue) >= len(substr)) &&
			(objValue == substr ||
				(len(objValue) > len(substr) &&
					(objValue[:len(substr)] == substr ||
						objValue[len(objValue)-len(substr):] == substr ||
						containsAny(objValue, substr))))

	case PredicateGT, PredicateLT, PredicateGTE, PredicateLTE, PredicateRange:
		objNum, err1 := strconv.ParseFloat(objValue, 64)
		thresholdNum, ok2 := filter.Value.(float64)

		if err1 != nil || !ok2 {
			return false
		}

		switch filter.Type {
		case PredicateGT:
			return objNum > thresholdNum
		case PredicateLT:
			return objNum < thresholdNum
		case PredicateGTE:
			return objNum >= thresholdNum
		case PredicateLTE:
			return objNum <= thresholdNum
		case PredicateRange:
			rangeVals, ok := filter.Value.([2]float64)
			if !ok || len(rangeVals) != 2 {
				return false
			}
			return objNum >= rangeVals[0] && objNum <= rangeVals[1]
		}
	}
	return false
}

func containsAny(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func (m *MEBStore) prepareScan(s, p, o string) (*scanOptions, error) {
	sID, pID, oID, sBound, pBound, oBound, err := m.resolveScanIDs(s, p, o)
	if err != nil {
		return nil, err
	}

	strategy := selectScanStrategy(sBound, pBound, oBound, sID, pID, oID)

	return &scanOptions{
		ctx:      context.Background(),
		s:        s,
		p:        p,
		o:        o,
		sID:      sID,
		pID:      pID,
		oID:      oID,
		sBound:   sBound,
		pBound:   pBound,
		oBound:   oBound,
		strategy: strategy,
	}, nil
}

func (m *MEBStore) scanImpl(opts *scanOptions, processFn func(*scanResult) (Fact, bool)) iter.Seq2[Fact, error] {
	return func(yield func(Fact, error) bool) {
		txn := m.db.NewTransaction(false)
		defer txn.Discard()

		scanCtx := opts.ctx
		if scanCtx == nil {
			scanCtx = context.Background()
		}

		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(opts.strategy.prefix); it.ValidForPrefix(opts.strategy.prefix); it.Next() {
			select {
			case <-scanCtx.Done():
				yield(Fact{}, scanCtx.Err())
				return
			default:
			}

			item := it.Item()
			key := item.Key()

			if len(key) != keys.TripleKeySize {
				continue
			}

			var result scanResult
			result.foundSID, result.foundPID, result.foundOID = keys.DecodeTripleKey(key)

			if opts.sBound && result.foundSID != opts.sID {
				continue
			}
			if opts.pBound && result.foundPID != opts.pID {
				continue
			}
			if opts.oBound && result.foundOID != opts.oID {
				continue
			}

			result.key = key
			fact, ok := processFn(&result)
			if !ok {
				return
			}

			if !yield(fact, nil) {
				return
			}
		}
	}
}

func (m *MEBStore) Scan(s, p, o string) iter.Seq2[Fact, error] {
	return m.ScanContext(context.Background(), s, p, o)
}

func (m *MEBStore) ScanContext(ctx context.Context, s, p, o string) iter.Seq2[Fact, error] {
	opts, err := m.prepareScanWithContext(ctx, s, p, o)
	if err != nil {
		return func(yield func(Fact, error) bool) {
			yield(Fact{}, err)
		}
	}

	return m.scanImpl(opts, func(r *scanResult) (Fact, bool) {
		var subject, predicate, objectStr string
		var err error

		if opts.sBound {
			subject = opts.s
		} else {
			subject, err = m.dict.GetString(r.foundSID)
			if err != nil {
				return Fact{}, false
			}
		}

		if opts.pBound {
			predicate = opts.p
		} else {
			predicate, err = m.dict.GetString(r.foundPID)
			if err != nil {
				return Fact{}, false
			}
		}

		if opts.oBound {
			objectStr = opts.o
		} else {
			objectStr, err = m.dict.GetString(r.foundOID)
			if err != nil {
				return Fact{}, false
			}
		}

		return Fact{
			Subject:   subject,
			Predicate: predicate,
			Object:    objectStr,
		}, true
	})
}

func (m *MEBStore) prepareScanWithContext(ctx context.Context, s, p, o string) (*scanOptions, error) {
	sID, pID, oID, sBound, pBound, oBound, err := m.resolveScanIDs(s, p, o)
	if err != nil {
		return nil, err
	}

	strategy := selectScanStrategy(sBound, pBound, oBound, sID, pID, oID)

	return &scanOptions{
		ctx:      ctx,
		s:        s,
		p:        p,
		o:        o,
		sID:      sID,
		pID:      pID,
		oID:      oID,
		sBound:   sBound,
		pBound:   pBound,
		oBound:   oBound,
		strategy: strategy,
	}, nil
}

func (m *MEBStore) ScanWithFilters(s, p, o string, filters []PredicateFilter) iter.Seq2[Fact, error] {
	return m.ScanWithFiltersContext(context.Background(), s, p, o, filters)
}

func (m *MEBStore) ScanWithFiltersContext(ctx context.Context, s, p, o string, filters []PredicateFilter) iter.Seq2[Fact, error] {
	opts, err := m.prepareScanWithContext(ctx, s, p, o)
	if err != nil {
		return func(yield func(Fact, error) bool) {
			yield(Fact{}, err)
		}
	}
	opts.filters = filters

	return m.scanImpl(opts, func(r *scanResult) (Fact, bool) {
		var subject, predicate, objectStr string
		var err error

		if opts.sBound {
			subject = opts.s
		} else {
			subject, err = m.dict.GetString(r.foundSID)
			if err != nil {
				return Fact{}, false
			}
		}

		if opts.pBound {
			predicate = opts.p
		} else {
			predicate, err = m.dict.GetString(r.foundPID)
			if err != nil {
				return Fact{}, false
			}
		}

		if opts.oBound {
			objectStr = opts.o
		} else {
			objectStr, err = m.dict.GetString(r.foundOID)
			if err != nil {
				return Fact{}, false
			}
		}

		for _, filter := range opts.filters {
			if !evaluatePredicateFilter(objectStr, filter) {
				return Fact{}, false
			}
		}

		return Fact{
			Subject:   subject,
			Predicate: predicate,
			Object:    objectStr,
		}, true
	})
}
