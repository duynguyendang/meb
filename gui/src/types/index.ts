export interface Fact {
  subject: string;
  predicate: string;
  object: string;
  graph: string;
}

export interface FactResponse {
  facts: Fact[];
  total: number;
}

export interface DocumentInfo {
  id: string;
  content?: string;
  metadata?: Record<string, unknown>;
}

export interface VectorSearchResult {
  id: string;
  score: number;
  key: string;
}

export interface GraphStats {
  totalFacts: number;
  totalDocuments: number;
  totalVectors: number;
  graphs: string[];
  predicates: string[];
}

export interface ScanOptions {
  subject?: string;
  predicate?: string;
  object?: string;
  graph?: string;
  limit?: number;
}

export interface AppState {
  // Connection
  apiBase: string;
  setApiBase: (url: string) => void;
  
  // Data
  stats: GraphStats | null;
  facts: Fact[];
  documents: DocumentInfo[];
  
  // Query
  scanOptions: ScanOptions;
  setScanOptions: (opts: ScanOptions) => void;
  queryResults: Fact[];
  isLoading: boolean;
  
  // Selection
  selectedFact: Fact | null;
  setSelectedFact: (fact: Fact | null) => void;
  selectedDocument: DocumentInfo | null;
  setSelectedDocument: (doc: DocumentInfo | null) => void;
  
  // View mode
  viewMode: 'facts' | 'documents' | 'graph' | 'vectors' | 'search';
  setViewMode: (mode: 'facts' | 'documents' | 'graph' | 'vectors' | 'search') => void;
  
  // Search
  searchTerm: string;
  setSearchTerm: (term: string) => void;
  searchResults: VectorSearchResult[];
  
  // Error
  error: string | null;
  setError: (err: string | null) => void;
}

export type ViewMode = 'facts' | 'documents' | 'graph' | 'vectors' | 'search';
