import { Fact, FactResponse, GraphStats, DocumentInfo, ScanOptions, VectorSearchResult } from '../types';

const DEFAULT_API_BASE = 'http://localhost:8080/v1';

async function fetchAPI(endpoint: string, options?: RequestInit): Promise<Response> {
  const apiBase = localStorage.getItem('meb_api_base') || DEFAULT_API_BASE;
  const url = `${apiBase}${endpoint}`;
  
  const response = await fetch(url, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...options?.headers,
    },
  });
  
  if (!response.ok) {
    throw new Error(`API Error: ${response.status} ${response.statusText}`);
  }
  
  return response;
}

export async function fetchStats(): Promise<GraphStats> {
  const response = await fetchAPI('/stats');
  return response.json();
}

export async function scanFacts(options: ScanOptions): Promise<FactResponse> {
  const params = new URLSearchParams();
  
  if (options.subject) params.append('subject', options.subject);
  if (options.predicate) params.append('predicate', options.predicate);
  if (options.object) params.append('object', options.object);
  if (options.graph) params.append('graph', options.graph);
  if (options.limit) params.append('limit', options.limit.toString());
  
  const response = await fetchAPI(`/facts?${params.toString()}`);
  return response.json();
}

export async function fetchDocuments(): Promise<DocumentInfo[]> {
  const response = await fetchAPI('/documents');
  return response.json();
}

export async function fetchDocument(id: string): Promise<DocumentInfo> {
  const response = await fetchAPI(`/documents/${encodeURIComponent(id)}`);
  return response.json();
}

export async function searchVectors(query: string, limit: number = 10): Promise<VectorSearchResult[]> {
  const response = await fetchAPI('/vectors/search', {
    method: 'POST',
    body: JSON.stringify({ query, limit }),
  });
  return response.json();
}

export async function fetchPredicates(): Promise<string[]> {
  const response = await fetchAPI('/predicates');
  return response.json();
}

export async function fetchGraphs(): Promise<string[]> {
  const response = await fetchAPI('/graphs');
  return response.json();
}

export async function fetchContent(id: string): Promise<string> {
  const response = await fetchAPI(`/content/${encodeURIComponent(id)}`);
  return response.text();
}

export function setApiBase(url: string): void {
  localStorage.setItem('meb_api_base', url);
}

export function getApiBase(): string {
  return localStorage.getItem('meb_api_base') || DEFAULT_API_BASE;
}
