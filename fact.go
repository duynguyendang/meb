package meb

import "fmt"

// Fact represents a single Quad (Subject-Predicate-Object-Graph) in the knowledge base.
// This format supports multi-tenancy and RAG contexts by including a Graph identifier.
type Fact struct {
	Subject   string // The subject entity
	Predicate string // The predicate/relation
	Object    any    // The object value (can be string, int, float64, bool, etc.)
	Graph     string // The graph/context identifier. Defaults to "default" if empty.
}

// String returns a human-readable representation of the Fact.
func (f Fact) String() string {
	graph := f.Graph
	if graph == "" {
		graph = "default"
	}
	return fmt.Sprintf("<%s, %s, %v> @%s", f.Subject, f.Predicate, f.Object, graph)
}

// IsValid checks if the fact has all required fields.
func (f Fact) IsValid() bool {
	return f.Subject != "" && f.Predicate != "" && f.Object != nil
}

// WithGraph returns a new Fact with the specified graph.
func (f Fact) WithGraph(graph string) Fact {
	f.Graph = graph
	return f
}

// NewFact creates a new Fact with the given subject, predicate, and object.
// The graph defaults to "default".
func NewFact(subject, predicate string, object any) Fact {
	return Fact{
		Subject:   subject,
		Predicate: predicate,
		Object:    object,
		Graph:     "default",
	}
}

// NewFactInGraph creates a new Fact in a specific graph.
func NewFactInGraph(subject, predicate string, object any, graph string) Fact {
	return Fact{
		Subject:   subject,
		Predicate: predicate,
		Object:    object,
		Graph:     graph,
	}
}
