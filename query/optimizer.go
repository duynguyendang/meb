package query

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
	"github.com/duynguyendang/meb/keys"
	"github.com/google/mangle/ast"
)

const (
	MaxJoinResults = 5000
	MaxJoinDepth   = 10
)

type CardinalityStats struct {
	Count       uint64
	LastUpdated int64
}

type QueryOptimizer struct {
	db          *badger.DB
	cardinality map[string]*CardinalityStats
}

func NewQueryOptimizer(db *badger.DB) *QueryOptimizer {
	return &QueryOptimizer{
		db:          db,
		cardinality: make(map[string]*CardinalityStats),
	}
}

type JoinPlan struct {
	Atoms         []OptimizedAtom
	Order         []int
	Cost          float64
	EstimatedRows uint64
}

type OptimizedAtom struct {
	Predicate   string
	Args        []ArgBinding
	Cardinality uint64
	Selectivity float64
}

type ArgBinding struct {
	Position int
	IsBound  bool
	Value    string
}

func (qo *QueryOptimizer) EstimateCardinality(predicate string) (uint64, error) {
	if stats, ok := qo.cardinality[predicate]; ok {
		return stats.Count, nil
	}

	count := qo.countFacts(predicate)

	qo.cardinality[predicate] = &CardinalityStats{
		Count: count,
	}

	return count, nil
}

func (qo *QueryOptimizer) countFacts(predicate string) uint64 {
	var count uint64

	qo.db.View(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		prefix := []byte{keys.QuadSPOGPrefix}
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()
			if len(key) == keys.QuadKeySize {
				count++
			}
		}
		return nil
	})

	return count
}

func (qo *QueryOptimizer) RefreshCardinality() error {
	for pred := range qo.cardinality {
		count := qo.countFacts(pred)
		atomic.StoreUint64(&qo.cardinality[pred].Count, count)
	}
	return nil
}

type QueryPlan struct {
	Atoms []AtomPlan
	Order []int
	Cost  float64
}

type AtomPlan struct {
	Predicate     string
	BoundArgs     []int
	FreeArgs      []int
	Cardinality   uint64
	Selectivity   float64
	EstimatedCost float64
}

func (qo *QueryOptimizer) CreatePlan(atoms []ast.Atom) (*QueryPlan, error) {
	atomPlans := make([]AtomPlan, len(atoms))

	for i, atom := range atoms {
		plan := AtomPlan{
			Predicate: atom.Predicate.Symbol,
		}

		cardinality, err := qo.EstimateCardinality(atom.Predicate.Symbol)
		if err != nil {
			cardinality = 1000
		}
		plan.Cardinality = cardinality

		plan.Selectivity = qo.estimateSelectivity(&plan)
		plan.EstimatedCost = float64(plan.Cardinality) * plan.Selectivity

		boundArgs := 0
		for j, arg := range atom.Args {
			if _, ok := arg.(ast.Constant); ok {
				plan.BoundArgs = append(plan.BoundArgs, j)
				boundArgs++
			} else {
				plan.FreeArgs = append(plan.FreeArgs, j)
			}
		}

		if boundArgs == len(atom.Args) {
			plan.Selectivity = 1.0 / float64(cardinality)
		}

		atomPlans[i] = plan
	}

	order := qo.greedyJoinOrder(atomPlans)

	return &QueryPlan{
		Atoms: atomPlans,
		Order: order,
		Cost:  qo.calculatePlanCost(atomPlans, order),
	}, nil
}

func (qo *QueryOptimizer) estimateSelectivity(plan *AtomPlan) float64 {
	if plan.Cardinality == 0 {
		return 0
	}

	boundRatio := float64(len(plan.BoundArgs)) / 3.0

	selectivity := math.Pow(0.01, boundRatio)

	return selectivity
}

func (qo *QueryOptimizer) greedyJoinOrder(atoms []AtomPlan) []int {
	n := len(atoms)
	if n == 0 {
		return nil
	}
	if n == 1 {
		return []int{0}
	}

	type joinCost struct {
		atomIndex int
		cost      float64
	}

	available := make(map[int]bool)
	for i := range atoms {
		available[i] = true
	}

	order := make([]int, 0, n)

	minCardIdx := 0
	minCard := atoms[0].Cardinality
	for i := 1; i < n; i++ {
		if atoms[i].Cardinality < minCard {
			minCard = atoms[i].Cardinality
			minCardIdx = i
		}
	}

	order = append(order, minCardIdx)
	delete(available, minCardIdx)

	for len(available) > 0 {
		currentAtom := atoms[order[len(order)-1]]
		bestIdx := -1
		bestCost := math.MaxFloat64

		for idx := range available {
			cost := qo.joinCost(atoms[idx], currentAtom, atoms)
			if cost < bestCost {
				bestCost = cost
				bestIdx = idx
			}
		}

		if bestIdx == -1 {
			for idx := range available {
				order = append(order, idx)
				break
			}
			break
		}

		order = append(order, bestIdx)
		delete(available, bestIdx)
	}

	return order
}

func (qo *QueryOptimizer) joinCost(atom1, atom2 AtomPlan, allAtoms []AtomPlan) float64 {
	sharedBound := 0
	for _, b1 := range atom1.BoundArgs {
		for _, b2 := range atom2.BoundArgs {
			if b1 == b2 {
				sharedBound++
			}
		}
	}

	if sharedBound > 0 {
		return float64(atom1.Cardinality) * atom1.Selectivity * 0.1
	}

	joinSelectivity := atom1.Selectivity * atom2.Selectivity
	return float64(atom1.Cardinality) * joinSelectivity
}

func (qo *QueryOptimizer) calculatePlanCost(atoms []AtomPlan, order []int) float64 {
	if len(order) == 0 {
		return 0
	}

	totalCost := 0.0

	currentCard := float64(atoms[order[0]].Cardinality)
	totalCost += currentCard

	for i := 1; i < len(order); i++ {
		atom := atoms[order[i]]

		joinSelectivity := qo.computeJoinSelectivity(atoms[order[i-1]], atom)

		stepCost := currentCard * joinSelectivity * float64(atom.Cardinality)
		totalCost += stepCost

		currentCard = currentCard * joinSelectivity * float64(atom.Cardinality)
		if currentCard > MaxJoinResults {
			currentCard = MaxJoinResults
		}
	}

	return totalCost
}

func (qo *QueryOptimizer) computeJoinSelectivity(atom1, atom2 AtomPlan) float64 {
	sharedVars := 0

	arg1Vars := make(map[int]bool)
	for _, b := range atom1.BoundArgs {
		arg1Vars[b] = true
	}

	for _, b := range atom2.BoundArgs {
		if arg1Vars[b] {
			sharedVars++
		}
	}

	if sharedVars > 0 {
		return 1.0 / math.Max(float64(atom1.Cardinality), 1)
	}

	return atom1.Selectivity * atom2.Selectivity
}

func (qo *QueryOptimizer) OptimizeDatalogQuery(query string) (*QueryPlan, error) {
	atoms, err := parseDatalogQuery(query)
	if err != nil {
		return nil, err
	}

	return qo.CreatePlan(atoms)
}

func parseDatalogQuery(query string) ([]ast.Atom, error) {
	var atoms []ast.Atom

	atomStrs := splitQueryAtoms(query)

	for _, atomStr := range atomStrs {
		atom, err := parseAtom(atomStr)
		if err != nil {
			continue
		}
		atoms = append(atoms, atom)
	}

	if len(atoms) == 0 {
		return nil, fmt.Errorf("no valid atoms in query")
	}

	return atoms, nil
}

func splitQueryAtoms(query string) []string {
	var atoms []string
	var current strings.Builder
	depth := 0

	for _, c := range query {
		switch c {
		case '(':
			depth++
			current.WriteRune(c)
		case ')':
			depth--
			current.WriteRune(c)
		case ',':
			if depth == 0 {
				if current.Len() > 0 {
					atoms = append(atoms, current.String())
					current.Reset()
				}
			} else {
				current.WriteRune(c)
			}
		default:
			if depth == 0 && (c == ' ' || c == '\t' || c == '\n') {
				if current.Len() > 0 {
					atoms = append(atoms, current.String())
					current.Reset()
				}
			} else {
				current.WriteRune(c)
			}
		}
	}

	if current.Len() > 0 {
		atoms = append(atoms, current.String())
	}

	return atoms
}

func parseAtom(atomStr string) (ast.Atom, error) {
	atomStr = strings.TrimSpace(atomStr)

	start := strings.Index(atomStr, "(")
	end := strings.LastIndex(atomStr, ")")

	if start == -1 || end == -1 || start >= end {
		return ast.Atom{}, fmt.Errorf("invalid atom format")
	}

	predicate := strings.TrimSpace(atomStr[:start])

	argsStr := atomStr[start+1 : end]
	var args []ast.BaseTerm

	argStrs := splitArgs(argsStr)
	for i, argStr := range argStrs {
		argStr = strings.TrimSpace(argStr)

		if strings.HasPrefix(argStr, "?") {
			args = append(args, ast.Constant{Type: ast.StringType, Symbol: argStr})
		} else if argStr == "_" {
			args = append(args, ast.Constant{Type: ast.StringType, Symbol: fmt.Sprintf("_%d", i)})
		} else {
			args = append(args, ast.Constant{Type: ast.StringType, Symbol: argStr})
		}
	}

	return ast.Atom{
		Predicate: ast.PredicateSym{Symbol: predicate},
		Args:      args,
	}, nil
}

func splitArgs(argsStr string) []string {
	var args []string
	var current strings.Builder
	depth := 0
	inQuote := false

	for _, c := range argsStr {
		switch c {
		case '"':
			inQuote = !inQuote
			current.WriteRune(c)
		case '(':
			depth++
			current.WriteRune(c)
		case ')':
			depth--
			current.WriteRune(c)
		case ',':
			if depth == 0 && !inQuote {
				args = append(args, current.String())
				current.Reset()
			} else {
				current.WriteRune(c)
			}
		default:
			current.WriteRune(c)
		}
	}

	if current.Len() > 0 {
		args = append(args, current.String())
	}

	return args
}

type QueryStats struct {
	PlanCost        float64
	ActualRows      uint64
	PlanningTimeUs  int64
	ExecutionTimeUs int64
}

func (qo *QueryOptimizer) GetStats() QueryStats {
	var totalCard uint64
	for _, stats := range qo.cardinality {
		totalCard += stats.Count
	}

	return QueryStats{
		PlanCost: float64(totalCard),
	}
}

type PredicateStats struct {
	Name           string
	Cardinality    uint64
	AvgSelectivity float64
	UsageCount     uint64
}

func (qo *QueryOptimizer) GetPredicateStats() []PredicateStats {
	stats := make([]PredicateStats, 0, len(qo.cardinality))

	for name, card := range qo.cardinality {
		stats = append(stats, PredicateStats{
			Name:        name,
			Cardinality: card.Count,
		})
	}

	sort.Slice(stats, func(i, j int) bool {
		return stats[i].Cardinality < stats[j].Cardinality
	})

	return stats
}

func (qo *QueryOptimizer) ExplainPlan(plan *QueryPlan) string {
	var explanation strings.Builder

	explanation.WriteString("Query Plan:\n")
	explanation.WriteString("=============\n\n")

	for i, idx := range plan.Order {
		atom := plan.Atoms[idx]
		explanation.WriteString(fmt.Sprintf("Step %d: %s\n", i+1, atom.Predicate))
		explanation.WriteString(fmt.Sprintf("  Cardinality: %d\n", atom.Cardinality))
		explanation.WriteString(fmt.Sprintf("  Selectivity: %.6f\n", atom.Selectivity))
		explanation.WriteString(fmt.Sprintf("  Bound Args: %v\n", atom.BoundArgs))
		explanation.WriteString(fmt.Sprintf("  Free Args: %v\n", atom.FreeArgs))
		explanation.WriteString("\n")
	}

	explanation.WriteString(fmt.Sprintf("Total Plan Cost: %.2f\n", plan.Cost))

	return explanation.String()
}
