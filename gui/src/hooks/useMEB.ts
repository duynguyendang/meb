import { useState, useCallback, useEffect } from 'react';
import { 
  fetchStats, 
  scanFacts, 
  fetchDocuments, 
  searchVectors, 
  fetchPredicates, 
  fetchGraphs 
} from '../services/api';
import { GraphStats, Fact, DocumentInfo, ScanOptions, VectorSearchResult } from '../types';

export function useMEB() {
  const [apiBase, setApiBase] = useState<string>(() => {
    return localStorage.getItem('meb_api_base') || 'http://localhost:8080/v1';
  });
  
  const [stats, setStats] = useState<GraphStats | null>(null);
  const [facts, setFacts] = useState<Fact[]>([]);
  const [documents, setDocuments] = useState<DocumentInfo[]>([]);
  const [scanOptions, setScanOptions] = useState<ScanOptions>({ limit: 100 });
  const [queryResults, setQueryResults] = useState<Fact[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [selectedFact, setSelectedFact] = useState<Fact | null>(null);
  const [selectedDocument, setSelectedDocument] = useState<DocumentInfo | null>(null);
  const [viewMode, setViewMode] = useState<'facts' | 'documents' | 'graph' | 'vectors' | 'search'>('facts');
  const [searchTerm, setSearchTerm] = useState('');
  const [searchResults, setSearchResults] = useState<VectorSearchResult[]>([]);
  const [error, setError] = useState<string | null>(null);
  
  const loadStats = useCallback(async () => {
    try {
      setIsLoading(true);
      const data = await fetchStats();
      setStats(data);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load stats');
    } finally {
      setIsLoading(false);
    }
  }, []);
  
  const runQuery = useCallback(async (options: ScanOptions) => {
    try {
      setIsLoading(true);
      const data = await scanFacts(options);
      setQueryResults(data.facts);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to query facts');
    } finally {
      setIsLoading(false);
    }
  }, []);
  
  const loadDocuments = useCallback(async () => {
    try {
      setIsLoading(true);
      const data = await fetchDocuments();
      setDocuments(data);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load documents');
    } finally {
      setIsLoading(false);
    }
  }, []);
  
  const runVectorSearch = useCallback(async (query: string) => {
    if (!query.trim()) return;
    
    try {
      setIsLoading(true);
      const results = await searchVectors(query, 20);
      setSearchResults(results);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Vector search failed');
    } finally {
      setIsLoading(false);
    }
  }, []);
  
  const updateApiBase = useCallback((url: string) => {
    setApiBase(url);
    localStorage.setItem('meb_api_base', url);
  }, []);
  
  // Initial load
  useEffect(() => {
    loadStats();
  }, [loadStats]);
  
  return {
    // Connection
    apiBase,
    setApiBase: updateApiBase,
    
    // Data
    stats,
    facts,
    documents,
    
    // Query
    scanOptions,
    setScanOptions,
    queryResults,
    isLoading,
    
    // Selection
    selectedFact,
    setSelectedFact,
    selectedDocument,
    setSelectedDocument,
    
    // View mode
    viewMode,
    setViewMode,
    
    // Search
    searchTerm,
    setSearchTerm,
    searchResults,
    
    // Error
    error,
    setError,
    
    // Actions
    loadStats,
    runQuery,
    loadDocuments,
    runVectorSearch,
  };
}
