package meb

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"codeberg.org/TauCeti/mangle-go/analysis"
	"codeberg.org/TauCeti/mangle-go/ast"
	"codeberg.org/TauCeti/mangle-go/engine"
	"codeberg.org/TauCeti/mangle-go/factstore"
	"codeberg.org/TauCeti/mangle-go/parse"
	"codeberg.org/TauCeti/mangle-go/provenance"
)

type mangleConfig struct {
	topicID          uint32
	provenance       bool
	createdFactLimit int
	maxProofs        int
	maxDepth         int
	persistTriples   bool
}

// MangleOption configures Mangle program evaluation and provenance explanation.
type MangleOption func(*mangleConfig)

// WithMangleTopic scopes Mangle evaluation and provenance to a specific TopicID.
func WithMangleTopic(topicID uint32) MangleOption {
	return func(c *mangleConfig) {
		c.topicID = topicID
	}
}

// WithMangleProvenance installs Mangle v0.7's DerivationRecorder during
// evaluation so full proof DAGs (including let/do transforms) can be built.
func WithMangleProvenance() MangleOption {
	return func(c *mangleConfig) {
		c.provenance = true
	}
}

// WithMangleCreatedFactLimit caps the number of derived facts created during
// Mangle evaluation.
func WithMangleCreatedFactLimit(limit int) MangleOption {
	return func(c *mangleConfig) {
		c.createdFactLimit = limit
	}
}

// WithMangleMaxProofs sets the maximum number of alternative proof trees
// returned by ExplainMangle / ExplainMangleAsFacts (default 1).
func WithMangleMaxProofs(n int) MangleOption {
	return func(c *mangleConfig) {
		c.maxProofs = n
	}
}

// WithMangleMaxDepth sets the maximum proof-tree depth (default 64).
func WithMangleMaxDepth(d int) MangleOption {
	return func(c *mangleConfig) {
		c.maxDepth = d
	}
}

// WithManglePersistTriples writes newly derived triples/3 facts back into the
// backing MEBStore instead of keeping all EDB triples read-only during eval.
// By default, EDB triples in MEBStore are read and derived predicates stay in
// the evaluation overlay unless persisted explicitly.
func WithManglePersistTriples(persist bool) MangleOption {
	return func(c *mangleConfig) {
		c.persistTriples = persist
	}
}

// MangleEvalResult holds the output of evaluating a Mangle program over MEBStore.
type MangleEvalResult struct {
	Stats    engine.Stats
	Program  *analysis.ProgramInfo
	Facts    factstore.FactStoreWithRemove
	Recorder *provenance.MemoryRecorder
}

// QueryAtoms queries the evaluated store using a Mangle atom pattern string
// (e.g. `reaches("a.go", X)` or `triples(S, "calls", O)`).
func (r *MangleEvalResult) QueryAtoms(atomPattern string) ([]ast.Atom, error) {
	q, err := parse.Atom(strings.TrimSpace(atomPattern))
	if err != nil {
		return nil, fmt.Errorf("parse atom %q: %w", atomPattern, err)
	}
	var out []ast.Atom
	if err := r.Facts.GetFacts(q, func(a ast.Atom) error {
		out = append(out, a)
		return nil
	}); err != nil {
		return nil, err
	}
	return out, nil
}

// EvalMangle parses, type/bounds-analyzes, stratifies, and evaluates a Mangle
// program over the topic-scoped MEBStore using Mangle v0.7's stratified engine.
//
// If the program does not declare `triples(S, P, O).`, the declaration is
// automatically prepended so rules can reference `triples(...)` directly.
func (m *MEBStore) EvalMangle(ctx context.Context, programSrc string, opts ...MangleOption) (*MangleEvalResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if m.db == nil || m.db.IsClosed() {
		return nil, ErrStoreClosed
	}

	cfg := mangleConfig{
		topicID: m.TopicID(),
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.topicID == 0 {
		cfg.topicID = m.TopicID()
	}

	src := ensureTriplesDecl(programSrc)
	unit, err := parse.Unit(strings.NewReader(src))
	if err != nil {
		return nil, fmt.Errorf("mangle parse: %w", err)
	}

	pi, err := analysis.AnalyzeOneUnit(unit, nil)
	if err != nil {
		return nil, fmt.Errorf("mangle analyze: %w", err)
	}

	strata, predToStratum, err := analysis.Stratify(analysis.Program{
		EdbPredicates: pi.EdbPredicates,
		IdbPredicates: pi.IdbPredicates,
		Rules:         pi.Rules,
	})
	if err != nil {
		return nil, fmt.Errorf("mangle stratify: %w", err)
	}

	fs := m.FactStoreForTopic(cfg.topicID)
	for _, f := range pi.InitialFacts {
		if f.Predicate == triplesPredSym && !cfg.persistTriples {
			fs.mu.Lock()
			fs.overlay.Add(f)
			fs.mu.Unlock()
			continue
		}
		fs.Add(f)
	}

	evalOpts := []engine.EvalOption{
		engine.WithDeterministicOrder(),
	}
	if cfg.createdFactLimit > 0 {
		evalOpts = append(evalOpts, engine.WithCreatedFactLimit(cfg.createdFactLimit))
	}

	var rec *provenance.MemoryRecorder
	if cfg.provenance {
		rec = provenance.NewMemoryRecorder()
		evalOpts = append(evalOpts, engine.WithDerivationRecorder(rec))
	}

	stats, err := engine.EvalStratifiedProgramWithStats(pi, strata, predToStratum, fs, evalOpts...)
	if err != nil {
		return nil, fmt.Errorf("mangle eval: %w", err)
	}

	return &MangleEvalResult{
		Stats:    stats,
		Program:  pi,
		Facts:    fs,
		Recorder: rec,
	}, nil
}

// ExplainMangle evaluates programSrc with full derivation recording enabled and
// returns proof DAGs for goalSrc (e.g. `reaches("main.go", "db.go")`), plus a
// human-readable proof tree string formatted via provenance.Print.
func (m *MEBStore) ExplainMangle(ctx context.Context, programSrc, goalSrc string, opts ...MangleOption) ([]*provenance.ProofNode, string, error) {
	goal, err := parse.Atom(strings.TrimSpace(goalSrc))
	if err != nil {
		return nil, "", fmt.Errorf("parse goal %q: %w", goalSrc, err)
	}

	cfg := mangleConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}

	evalOpts := append([]MangleOption{}, opts...)
	evalOpts = append(evalOpts, WithMangleProvenance())

	res, err := m.EvalMangle(ctx, programSrc, evalOpts...)
	if err != nil {
		return nil, "", err
	}

	provOpts := provenance.Options{
		MaxProofs: cfg.maxProofs,
		MaxDepth:  cfg.maxDepth,
	}

	proofs, err := provenance.BuildFromRecording(res.Recorder, res.Facts, goal, provOpts)
	if err != nil {
		return nil, "", err
	}

	var buf bytes.Buffer
	if err := provenance.Print(&buf, proofs); err != nil {
		return nil, "", err
	}
	return proofs, buf.String(), nil
}

// ExplainMangleAsFacts runs ExplainMangle and emits the resulting proof DAG as
// queryable Mangle facts (proves, uses_rule, premise, edb_leaf, absence_leaf,
// binding, rule_source, uses_transform, group_key) into an in-memory FactStore.
func (m *MEBStore) ExplainMangleAsFacts(ctx context.Context, programSrc, goalSrc string, opts ...MangleOption) (factstore.FactStore, []*provenance.ProofNode, error) {
	proofs, _, err := m.ExplainMangle(ctx, programSrc, goalSrc, opts...)
	if err != nil {
		return nil, nil, err
	}
	out := factstore.NewSimpleInMemoryStore()
	if err := provenance.EmitFacts(proofs, out); err != nil {
		return nil, nil, err
	}
	return out, proofs, nil
}

func ensureTriplesDecl(programSrc string) string {
	if strings.Contains(programSrc, "Decl triples(") {
		return programSrc
	}
	return "Decl triples(S, P, O).\n" + programSrc
}
