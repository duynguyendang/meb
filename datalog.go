package meb

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/duynguyendang/meb/keys"
	"github.com/duynguyendang/meb/query"
)

// QueryOption configures query execution parameters.
type QueryOption func(*queryOptions)

type queryOptions struct {
	limit int
	cache *query.QueryCache
}

// WithLimit sets the maximum number of rows to return (0 = unlimited).
func WithLimit(limit int) QueryOption {
	return func(o *queryOptions) {
		o.limit = limit
	}
}

// WithCache sets the QueryCache instance to use for memoizing results.
func WithCache(cache *query.QueryCache) QueryOption {
	return func(o *queryOptions) {
		o.cache = cache
	}
}

// Query executes a Datalog query string on the store, supporting:
// - Triples atoms: `triples(?S, "calls", ?O)`
// - Syntactic sugar: `?S != ?O`, `?A = ?B`, `?A > ?B`, `?A >= ?B`, etc.
// - Constraints: `neq`, `eq`, `gt`, `gte`, `lt`, `lte`, `contains`, `regex`, and `not(triples(...))`
// - Multi-way joins using worst-case optimal Leapfrog Triejoin (LFTJ) with sequential join fallback
// - Optional result limit and query caching
func (m *MEBStore) Query(ctx context.Context, q string, opts ...QueryOption) ([]map[string]any, error) {
	if m.db == nil || m.db.IsClosed() {
		return nil, ErrStoreClosed
	}

	options := queryOptions{
		limit: 1000, // default limit
	}
	for _, opt := range opts {
		opt(&options)
	}

	var cacheKey string
	if options.cache != nil {
		cacheKey = options.cache.HashKey(fmt.Sprintf("%d|%d|%s", m.TopicID(), options.limit, q))
		if cached, ok := options.cache.Get(cacheKey); ok {
			if options.limit > 0 && len(cached) > options.limit {
				return cached[:options.limit], nil
			}
			return cached, nil
		}
	}

	atoms, err := query.ParseDatalog(q)
	if err != nil {
		return nil, fmt.Errorf("failed to parse datalog query: %w", err)
	}
	if len(atoms) == 0 {
		return nil, fmt.Errorf("empty query")
	}

	triplesAtoms := make([]query.Atom, 0, len(atoms))
	constraintAtoms := make([]query.Atom, 0)

	for _, atom := range atoms {
		if atom.Predicate == "triples" {
			triplesAtoms = append(triplesAtoms, atom)
		} else {
			constraintAtoms = append(constraintAtoms, atom)
		}
	}

	if len(triplesAtoms) == 0 {
		return nil, fmt.Errorf("query must contain at least one triples atom")
	}

	for _, atom := range constraintAtoms {
		if err := m.checkConstraintAtom(atom); err != nil {
			return nil, err
		}
	}

	execQuery := func(execCtx context.Context) ([]map[string]any, error) {
		internalLimit := options.limit
		if len(constraintAtoms) > 0 {
			// Enforce limit after constraints so pre-truncation does not drop matching rows.
			internalLimit = 0
		}

		var results []map[string]any
		var execErr error
		if len(triplesAtoms) == 1 {
			results, execErr = m.executeSingleAtomQuery(execCtx, triplesAtoms[0], internalLimit)
		} else {
			results, execErr = m.executeLFTJQuery(execCtx, triplesAtoms, internalLimit)
			if execErr == nil && len(results) == 0 && len(triplesAtoms) > 1 {
				results, execErr = m.executeSequentialJoinQuery(execCtx, triplesAtoms, internalLimit)
			}
		}
		if execErr != nil {
			return nil, execErr
		}

		results, err = m.applyConstraints(execCtx, results, constraintAtoms)
		if err != nil {
			return nil, err
		}

		if options.limit > 0 && len(results) > options.limit {
			results = results[:options.limit]
		}
		return results, nil
	}

	var results []map[string]any
	if breaker := m.breaker.Load(); breaker != nil {
		raw, err := breaker.ExecuteWithResult(ctx, func(execCtx context.Context) (interface{}, error) {
			return execQuery(execCtx)
		})
		if err != nil {
			return nil, err
		}
		results, _ = raw.([]map[string]any)
	} else {
		results, err = execQuery(ctx)
		if err != nil {
			return nil, err
		}
	}

	if options.cache != nil {
		options.cache.Set(cacheKey, results)
	}

	return results, nil
}

func (m *MEBStore) executeSingleAtomQuery(ctx context.Context, atom query.Atom, limit int) ([]map[string]any, error) {
	var results []map[string]any

	subj := resolveDatalogArg(atom.Args[0])
	pred := resolveDatalogArg(atom.Args[1])
	obj := resolveDatalogArg(atom.Args[2])

	subjIsVar := isDatalogVariable(atom.Args[0])
	predIsVar := isDatalogVariable(atom.Args[1])
	objIsVar := isDatalogVariable(atom.Args[2])

	for fact, err := range m.ScanContext(ctx, subj, pred, obj) {
		if err != nil {
			return nil, err
		}

		result := make(map[string]any)
		if subjIsVar {
			result[atom.Args[0]] = fact.Subject
		}
		if predIsVar {
			result[atom.Args[1]] = fact.Predicate
		}
		if objIsVar {
			result[atom.Args[2]] = fact.Object
		}

		if len(result) > 0 {
			results = append(results, result)
			if limit > 0 && len(results) >= limit {
				break
			}
		}
	}

	return results, nil
}

func (m *MEBStore) executeLFTJQuery(ctx context.Context, atoms []query.Atom, limit int) ([]map[string]any, error) {
	var results []map[string]any

	relations, resultVars, err := m.buildLFTJRelations(atoms)
	if err != nil || len(relations) == 0 {
		return results, err
	}

	engine := m.LFTJEngine()
	if engine == nil {
		return results, nil
	}

	boundVars := make(map[string]uint64)

	for joinResult, err := range engine.Execute(ctx, relations, boundVars, resultVars) {
		if err != nil {
			return nil, err
		}

		row := make(map[string]any)
		for varName, dictID := range joinResult {
			if dictID == 0 {
				continue
			}

			localID := keys.UnpackLocalID(dictID)
			strVal, err := m.ResolveID(localID)
			if err != nil {
				continue
			}
			row[varName] = strVal
		}

		if len(row) > 0 {
			results = append(results, row)
			if limit > 0 && len(results) >= limit {
				break
			}
		}
	}

	return results, nil
}

func (m *MEBStore) executeSequentialJoinQuery(ctx context.Context, atoms []query.Atom, limit int) ([]map[string]any, error) {
	var results []map[string]any

	firstAtom := atoms[0]
	subj := resolveDatalogArg(firstAtom.Args[0])
	pred := resolveDatalogArg(firstAtom.Args[1])
	obj := resolveDatalogArg(firstAtom.Args[2])

	for fact, err := range m.ScanContext(ctx, subj, pred, obj) {
		if err != nil {
			return nil, err
		}

		row := make(map[string]any)
		if isDatalogVariable(firstAtom.Args[0]) {
			row[firstAtom.Args[0]] = fact.Subject
		}
		if isDatalogVariable(firstAtom.Args[1]) {
			row[firstAtom.Args[1]] = fact.Predicate
		}
		if isDatalogVariable(firstAtom.Args[2]) {
			row[firstAtom.Args[2]] = fact.Object
		}

		for _, atom := range atoms[1:] {
			resolvedArgs := make([]string, 3)
			for i, arg := range atom.Args[:3] {
				if isDatalogVariable(arg) {
					if val, ok := row[arg]; ok {
						resolvedArgs[i] = fmt.Sprintf("%v", val)
					}
				} else {
					resolvedArgs[i] = resolveDatalogArg(arg)
				}
			}

			found := false
			for f, err := range m.ScanContext(ctx, resolvedArgs[0], resolvedArgs[1], resolvedArgs[2]) {
				if err != nil {
					return nil, err
				}
				if isDatalogVariable(atom.Args[0]) {
					row[atom.Args[0]] = f.Subject
				}
				if isDatalogVariable(atom.Args[1]) {
					row[atom.Args[1]] = f.Predicate
				}
				if isDatalogVariable(atom.Args[2]) {
					row[atom.Args[2]] = f.Object
				}
				found = true
				break
			}
			if !found {
				goto nextFact
			}
		}

		if len(row) > 0 {
			results = append(results, row)
			if limit > 0 && len(results) >= limit {
				break
			}
		}
	nextFact:
	}

	return results, nil
}

func (m *MEBStore) buildLFTJRelations(atoms []query.Atom) ([]query.RelationPattern, []string, error) {
	relations := make([]query.RelationPattern, 0, len(atoms))
	resultVarsSet := make(map[string]bool)
	topicID := m.TopicID()

	for _, atom := range atoms {
		if atom.Predicate != "triples" || len(atom.Args) < 3 {
			continue
		}

		boundPositions := make(map[int]uint64)
		variablePositions := make(map[int]string)
		skipAtom := false

		for argIdx, arg := range atom.Args[:3] {
			if isDatalogVariable(arg) {
				variablePositions[argIdx] = arg
				resultVarsSet[arg] = true
			} else {
				strVal := resolveDatalogArg(arg)
				if strVal == "" {
					continue
				}
				dictID, found := m.LookupID(strVal)
				if !found {
					skipAtom = true
					break
				}
				packedID := keys.PackID(topicID, dictID)
				boundPositions[argIdx] = packedID
			}
		}

		if skipAtom {
			continue
		}

		relations = append(relations, query.RelationPattern{
			Prefix:            keys.TripleSPOPrefix,
			BoundPositions:    boundPositions,
			VariablePositions: variablePositions,
		})
	}

	resultVars := make([]string, 0, len(resultVarsSet))
	for v := range resultVarsSet {
		resultVars = append(resultVars, v)
	}

	return relations, resultVars, nil
}

func (m *MEBStore) applyConstraints(ctx context.Context, results []map[string]any, constraints []query.Atom) ([]map[string]any, error) {
	if len(constraints) == 0 {
		return results, nil
	}

	for _, atom := range constraints {
		if err := m.checkConstraintAtom(atom); err != nil {
			return nil, err
		}
	}

	filtered := make([]map[string]any, 0, len(results))
	for _, result := range results {
		ok, err := m.matchesConstraints(ctx, result, constraints)
		if err != nil {
			return nil, err
		}
		if ok {
			filtered = append(filtered, result)
		}
	}

	return filtered, nil
}

func (m *MEBStore) checkConstraintAtom(atom query.Atom) error {
	pred := atom.Predicate
	if _, ok := datalogConstraintCheckers[pred]; ok {
		return nil
	}

	switch {
	case pred == "contains" || pred == "regex":
		if len(atom.Args) < 2 {
			return fmt.Errorf("constraint %s(...) requires 2 arguments", pred)
		}
		return nil
	case pred == "not":
		inner, err := notInnerDatalogAtom(atom)
		if err != nil {
			return err
		}
		return m.checkNotInner(inner)
	case strings.HasPrefix(pred, "not "):
		return m.checkNotInner(query.Atom{
			Predicate: strings.TrimSpace(strings.TrimPrefix(pred, "not ")),
			Args:      atom.Args,
		})
	case pred == "or":
		return fmt.Errorf("constraint or(...) is not supported directly by meb query layer")
	default:
		return fmt.Errorf("unsupported constraint atom %s(%v)", pred, atom.Args)
	}
}

func (m *MEBStore) checkNotInner(inner query.Atom) error {
	if inner.Predicate == "triples" {
		return nil
	}
	if _, ok := datalogConstraintCheckers[inner.Predicate]; ok {
		return nil
	}
	switch inner.Predicate {
	case "contains", "regex", "not", "or":
		return m.checkConstraintAtom(inner)
	default:
		return fmt.Errorf("cannot evaluate not(%s(...)): derived predicate not stored as facts", inner.Predicate)
	}
}

func notInnerDatalogAtom(atom query.Atom) (query.Atom, error) {
	pred := atom.Predicate
	if strings.HasPrefix(pred, "not ") {
		inner := strings.TrimSpace(strings.TrimPrefix(pred, "not "))
		if inner == "" {
			return query.Atom{}, fmt.Errorf("malformed not constraint %s(...)", pred)
		}
		return query.Atom{Predicate: inner, Args: atom.Args}, nil
	}
	if len(atom.Args) == 0 {
		return query.Atom{}, fmt.Errorf("malformed not() constraint with no arguments")
	}
	innerAtoms, err := query.ParseDatalog(atom.Args[0])
	if err != nil {
		return query.Atom{}, fmt.Errorf("failed to parse inner atom of not(): %w", err)
	}
	if len(innerAtoms) != 1 {
		return query.Atom{}, fmt.Errorf("not() must wrap exactly one atom, got %d", len(innerAtoms))
	}
	return innerAtoms[0], nil
}

func (m *MEBStore) matchesConstraints(ctx context.Context, result map[string]any, constraints []query.Atom) (bool, error) {
	for _, atom := range constraints {
		ok, err := m.evalConstraint(ctx, result, atom)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

func (m *MEBStore) evalConstraint(ctx context.Context, result map[string]any, atom query.Atom) (bool, error) {
	pred := atom.Predicate

	if checker, ok := datalogConstraintCheckers[pred]; ok {
		if len(atom.Args) < 2 {
			return false, fmt.Errorf("constraint %s(...) requires 2 arguments", pred)
		}
		val, ok := result[atom.Args[0]]
		if !ok {
			return false, nil
		}
		return checker(val, atom.Args[1]), nil
	}

	switch {
	case pred == "contains" || pred == "regex":
		return evalDatalogStringConstraint(result, atom)
	case pred == "not":
		inner, err := notInnerDatalogAtom(atom)
		if err != nil {
			return false, err
		}
		return m.evalNegation(ctx, result, inner)
	case strings.HasPrefix(pred, "not "):
		return m.evalNegation(ctx, result, query.Atom{
			Predicate: strings.TrimSpace(strings.TrimPrefix(pred, "not ")),
			Args:      atom.Args,
		})
	default:
		return false, fmt.Errorf("unsupported constraint %s", pred)
	}
}

func (m *MEBStore) evalNegation(ctx context.Context, result map[string]any, inner query.Atom) (bool, error) {
	if inner.Predicate == "triples" {
		subj, pred, obj := m.scanDatalogPattern(result, inner.Args)
		exists, err := m.factExists(ctx, subj, pred, obj)
		if err != nil {
			return false, err
		}
		return !exists, nil
	}
	matched, err := m.evalConstraint(ctx, result, inner)
	if err != nil {
		return false, err
	}
	return !matched, nil
}

func (m *MEBStore) scanDatalogPattern(result map[string]any, args []string) (string, string, string) {
	pat := make([]string, 3)
	for i := 0; i < 3 && i < len(args); i++ {
		arg := args[i]
		if isDatalogVariable(arg) {
			if val, ok := result[arg]; ok {
				pat[i] = fmt.Sprintf("%v", val)
			}
		} else if arg != "_" {
			pat[i] = resolveDatalogArg(arg)
		}
	}
	return pat[0], pat[1], pat[2]
}

func (m *MEBStore) factExists(ctx context.Context, subj, pred, obj string) (bool, error) {
	for _, err := range m.ScanContext(ctx, subj, pred, obj) {
		if err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func evalDatalogStringConstraint(result map[string]any, atom query.Atom) (bool, error) {
	if len(atom.Args) < 2 {
		return false, fmt.Errorf("constraint %s(...) requires 2 arguments", atom.Predicate)
	}
	val, ok := result[atom.Args[0]]
	if !ok {
		return false, nil
	}
	s := fmt.Sprintf("%v", val)

	switch atom.Predicate {
	case "contains":
		return strings.Contains(s, atom.Args[1]), nil
	case "regex":
		re, err := regexp.Compile(atom.Args[1])
		if err != nil {
			return false, fmt.Errorf("invalid regex %q in regex(%s, ...): %w", atom.Args[1], atom.Args[0], err)
		}
		return re.MatchString(s), nil
	}
	return false, fmt.Errorf("unknown string constraint %s", atom.Predicate)
}

var datalogConstraintCheckers = map[string]func(val any, constraint string) bool{
	"neq": func(val any, c string) bool { return fmt.Sprintf("%v", val) != c },
	"!=":  func(val any, c string) bool { return fmt.Sprintf("%v", val) != c },
	"eq":  func(val any, c string) bool { return fmt.Sprintf("%v", val) == c },
	"=":   func(val any, c string) bool { return fmt.Sprintf("%v", val) == c },
	"gt":  func(val any, c string) bool { return compareDatalogNumeric(val, c, ">") },
	"gte": func(val any, c string) bool { return compareDatalogNumeric(val, c, ">=") },
	"lt":  func(val any, c string) bool { return compareDatalogNumeric(val, c, "<") },
	"lte": func(val any, c string) bool { return compareDatalogNumeric(val, c, "<=") },
}

func compareDatalogNumeric(val any, constraintStr string, op string) bool {
	valNum, valOk := toDatalogFloat64(val)
	constraintNum, constraintOk := toDatalogFloat64(constraintStr)
	if valOk && constraintOk {
		switch op {
		case ">":
			return valNum > constraintNum
		case ">=":
			return valNum >= constraintNum
		case "<":
			return valNum < constraintNum
		case "<=":
			return valNum <= constraintNum
		}
	}
	sVal := fmt.Sprintf("%v", val)
	switch op {
	case ">":
		return sVal > constraintStr
	case ">=":
		return sVal >= constraintStr
	case "<":
		return sVal < constraintStr
	case "<=":
		return sVal <= constraintStr
	}
	return false
}

func toDatalogFloat64(v any) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case int32:
		return float64(val), true
	case uint64:
		return float64(val), true
	case uint32:
		return float64(val), true
	case string:
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

func isDatalogVariable(arg string) bool {
	return len(arg) > 0 && (arg[0] == '?' || (arg[0] >= 'A' && arg[0] <= 'Z'))
}

func resolveDatalogArg(arg string) string {
	if arg == "_" {
		return ""
	}
	if len(arg) >= 2 && arg[0] == '"' && arg[len(arg)-1] == '"' {
		return arg[1 : len(arg)-1]
	}
	if len(arg) >= 1 && arg[0] == '?' {
		return ""
	}
	if len(arg) > 0 && arg[0] >= 'A' && arg[0] <= 'Z' {
		return ""
	}
	return arg
}
