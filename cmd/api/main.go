package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/duynguyendang/meb"
	"github.com/duynguyendang/meb/store"
)

var (
	port    = flag.String("port", "8080", "HTTP server port")
	dataDir = flag.String("data", "./data", "MEB data directory")
)

type Server struct {
	store *meb.MEBStore
	mux   *http.ServeMux
}

type FactResponse struct {
	Facts []Fact `json:"facts"`
	Total int    `json:"total"`
}

type Fact struct {
	Subject   string `json:"subject"`
	Predicate string `json:"predicate"`
	Object    string `json:"object"`
	Graph     string `json:"graph"`
}

type DocumentInfo struct {
	ID       string                 `json:"id"`
	Content  string                 `json:"content,omitempty"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

type VectorSearchRequest struct {
	Query string `json:"query"`
	Limit int    `json:"limit"`
}

type VectorSearchResult struct {
	ID    string  `json:"id"`
	Key   string  `json:"key"`
	Score float32 `json:"score"`
}

type GraphStats struct {
	TotalFacts     uint64   `json:"totalFacts"`
	TotalDocuments int      `json:"totalDocuments"`
	TotalVectors   int      `json:"totalVectors"`
	Graphs         []string `json:"graphs"`
	Predicates     []string `json:"predicates"`
}

func main() {
	flag.Parse()

	slog.Info("starting MEB API server", "port", *port, "dataDir", *dataDir)

	// Create MEB store
	cfg := store.DefaultConfig(*dataDir)
	s, err := meb.NewMEBStore(cfg)
	if err != nil {
		slog.Error("failed to create MEB store", "error", err)
		os.Exit(1)
	}
	defer s.Close()

	server := &Server{store: s, mux: http.NewServeMux()}
	server.registerRoutes()

	// Start server
	addr := ":" + *port
	slog.Info("server listening", "address", addr)

	go func() {
		if err := http.ListenAndServe(addr, server.mux); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	// Wait for interrupt
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch

	slog.Info("shutting down...")
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("/v1/stats", s.handleStats)
	s.mux.HandleFunc("/v1/facts", s.handleFacts)
	s.mux.HandleFunc("/v1/documents", s.handleDocuments)
	s.mux.HandleFunc("/v1/documents/", s.handleDocument)
	s.mux.HandleFunc("/v1/predicates", s.handlePredicates)
	s.mux.HandleFunc("/v1/graphs", s.handleGraphs)
	s.mux.HandleFunc("/v1/content/", s.handleContent)
	s.mux.HandleFunc("/v1/vectors/search", s.handleVectorSearch)

	// Health check
	s.mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats := GraphStats{
		TotalFacts:   s.store.Count(),
		TotalVectors: int(s.store.Vectors().Count()),
		Graphs:       []string{"default", "metadata"},
		Predicates:   []string{"triples"},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

func (s *Server) handleFacts(w http.ResponseWriter, r *http.Request) {
	subject := r.URL.Query().Get("subject")
	predicate := r.URL.Query().Get("predicate")
	object := r.URL.Query().Get("object")
	graph := r.URL.Query().Get("graph")
	limit := 100
	if l := r.URL.Query().Get("limit"); l != "" {
		fmt.Sscanf(l, "%d", &limit)
	}

	facts := make([]Fact, 0, limit)
	total := 0

	for f := range s.store.Scan(subject, predicate, object, graph) {
		objStr, _ := f.Object.(string)
		facts = append(facts, Fact{
			Subject:   f.Subject,
			Predicate: f.Predicate,
			Object:    objStr,
			Graph:     f.Graph,
		})
		total++
		if len(facts) >= limit {
			break
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(FactResponse{Facts: facts, Total: total})
}

func (s *Server) handleDocuments(w http.ResponseWriter, r *http.Request) {
	// Get all unique subjects that have content
	docs := make(map[string]bool)

	for f := range s.store.Scan("", "", "", "") {
		// Simple heuristic: subjects with "type" = "document"
		if f.Predicate == "type" {
			if objStr, ok := f.Object.(string); ok && objStr == "document" {
				docs[f.Subject] = true
			}
		}
	}

	documents := make([]DocumentInfo, 0, len(docs))
	for id := range docs {
		metadata, _ := s.store.GetDocumentMetadata(id)
		documents = append(documents, DocumentInfo{
			ID:       id,
			Metadata: metadata,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(documents)
}

func (s *Server) handleDocument(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Path[len("/v1/documents/"):]
	if id == "" {
		http.Error(w, "document ID required", http.StatusBadRequest)
		return
	}

	content, err := s.store.GetContentByKey(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	metadata, _ := s.store.GetDocumentMetadata(id)

	doc := DocumentInfo{
		ID:       id,
		Content:  string(content),
		Metadata: metadata,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(doc)
}

func (s *Server) handlePredicates(w http.ResponseWriter, r *http.Request) {
	predicates := []string{"triples"}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(predicates)
}

func (s *Server) handleGraphs(w http.ResponseWriter, r *http.Request) {
	graphs := []string{"default", "metadata"}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(graphs)
}

func (s *Server) handleContent(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Path[len("/v1/content/"):]
	if id == "" {
		http.Error(w, "content ID required", http.StatusBadRequest)
		return
	}

	content, err := s.store.GetContentByKey(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	if content != nil {
		w.Write(content)
	}
}

func (s *Server) handleVectorSearch(w http.ResponseWriter, r *http.Request) {
	var req VectorSearchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	limit := req.Limit
	if limit <= 0 {
		limit = 10
	}

	// For now, return empty results - would need embedding service
	// In production, you'd call an embedding API
	results := []VectorSearchResult{}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

// ServeHTTP implements http.Handler
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}
