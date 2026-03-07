import React, { useState } from 'react';
import { Loader2, Search, Zap } from 'lucide-react';

interface VectorSearchPanelProps {
  searchTerm: string;
  setSearchTerm: (term: string) => void;
  searchResults: { id: string; score: number; key: string }[];
  isLoading: boolean;
  runVectorSearch: (query: string) => void;
}

export function VectorSearchPanel({
  searchTerm,
  setSearchTerm,
  searchResults,
  isLoading,
  runVectorSearch
}: VectorSearchPanelProps) {
  const [inputValue, setInputValue] = useState(searchTerm);
  
  const handleSearch = () => {
    if (inputValue.trim()) {
      setSearchTerm(inputValue);
      runVectorSearch(inputValue);
    }
  };
  
  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      handleSearch();
    }
  };
  
  return (
    <div className="flex flex-col h-full">
      {/* Search Input */}
      <div className="p-4 border-b border-[var(--border)] bg-[var(--bg-surface)]">
        <div className="flex gap-2">
          <div className="flex-1 relative">
            <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-500" />
            <input
              type="text"
              value={inputValue}
              onChange={(e) => setInputValue(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder="Search by semantic similarity..."
              className="w-full bg-[var(--bg-main)] border border-[var(--border)] rounded-lg pl-10 pr-4 py-2.5 text-xs text-slate-200 focus:outline-none focus:border-[var(--accent-purple)] font-mono"
            />
          </div>
          <button
            onClick={handleSearch}
            disabled={isLoading || !inputValue.trim()}
            className="px-4 py-2 bg-[var(--accent-purple)] hover:bg-[var(--accent-purple)]/80 rounded-lg text-[10px] text-white font-black uppercase tracking-wider transition-colors disabled:opacity-50"
          >
            {isLoading ? <Loader2 size={14} className="animate-spin" /> : 'Search'}
          </button>
        </div>
      </div>
      
      {/* Results */}
      <div className="flex-1 overflow-auto custom-scrollbar">
        {searchResults.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-full text-slate-600">
            <Zap size={40} className="mb-3 opacity-30" />
            <p className="text-xs">
              {searchTerm ? 'No results found' : 'Enter a query to search vectors'}
            </p>
            <p className="text-[10px] mt-1 opacity-60">
              Vector search finds semantically similar nodes
            </p>
          </div>
        ) : (
          <div className="p-4">
            <div className="mb-3 text-[10px] text-slate-500 font-medium">
              Found {searchResults.length} similar nodes
            </div>
            
            <div className="space-y-2">
              {searchResults.map((result, idx) => (
                <div
                  key={`${result.id}-${idx}`}
                  className="p-3 bg-[var(--bg-surface)] border border-[var(--border)] rounded-lg hover:border-[var(--accent-purple)]/50 transition-colors cursor-pointer"
                >
                  <div className="flex items-center justify-between mb-2">
                    <span className="text-xs font-mono text-[var(--accent-blue)] truncate">
                      {result.key || result.id}
                    </span>
                    <span className="text-[10px] font-mono text-[var(--accent-purple)]">
                      {(result.score * 100).toFixed(1)}%
                    </span>
                  </div>
                  <div className="h-1.5 bg-[var(--bg-main)] rounded-full overflow-hidden">
                    <div
                      className="h-full bg-[var(--accent-purple)] rounded-full"
                      style={{ width: `${result.score * 100}%` }}
                    />
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
