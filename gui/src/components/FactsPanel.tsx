import React, { useEffect, useState } from 'react';
import { Loader2, ChevronDown, ChevronRight, X } from 'lucide-react';
import { Fact, ScanOptions } from '../types';
import { scanFacts } from '../services/api';

interface FactsPanelProps {
  scanOptions: ScanOptions;
  setScanOptions: (opts: ScanOptions) => void;
  selectedFact: Fact | null;
  setSelectedFact: (fact: Fact | null) => void;
  isLoading: boolean;
  runQuery: (opts: ScanOptions) => void;
}

export function FactsPanel({ 
  scanOptions, 
  setScanOptions, 
  selectedFact, 
  setSelectedFact,
  isLoading,
  runQuery 
}: FactsPanelProps) {
  const [results, setResults] = useState<Fact[]>([]);
  const [total, setTotal] = useState(0);
  const [showFilters, setShowFilters] = useState(true);
  
  useEffect(() => {
    const load = async () => {
      try {
        const data = await scanFacts(scanOptions);
        setResults(data.facts);
        setTotal(data.total);
      } catch (e) {
        console.error('Failed to load facts:', e);
      }
    };
    load();
  }, [scanOptions]);
  
  const updateFilter = (key: keyof ScanOptions, value: string) => {
    setScanOptions({
      ...scanOptions,
      [key]: value || undefined,
    });
  };
  
  const clearFilters = () => {
    setScanOptions({ limit: 100 });
  };
  
  return (
    <div className="flex h-full">
      {/* Filters Panel */}
      <div className={`border-r border-[var(--border)] bg-[var(--bg-surface)] transition-all ${showFilters ? 'w-72' : 'w-0 overflow-hidden'}`}>
        <div className="p-4 space-y-4">
          <div className="flex items-center justify-between">
            <h3 className="text-[10px] font-black uppercase tracking-[0.15em] text-slate-500">
              Filters
            </h3>
            <button
              onClick={clearFilters}
              className="text-[9px] text-[var(--accent-blue)] hover:underline font-medium"
            >
              Clear All
            </button>
          </div>
          
          <div className="space-y-3">
            <div>
              <label className="text-[9px] text-[var(--text-muted)] uppercase tracking-wider block mb-1.5 font-medium">
                Subject
              </label>
              <input
                type="text"
                value={scanOptions.subject || ''}
                onChange={(e) => updateFilter('subject', e.target.value)}
                placeholder="e.g., alice"
                className="w-full bg-[var(--bg-main)] border border-[var(--border)] rounded px-3 py-2 text-xs text-slate-200 focus:outline-none focus:border-[var(--accent-blue)] font-mono"
              />
            </div>
            
            <div>
              <label className="text-[9px] text-[var(--text-muted)] uppercase tracking-wider block mb-1.5 font-medium">
                Predicate
              </label>
              <input
                type="text"
                value={scanOptions.predicate || ''}
                onChange={(e) => updateFilter('predicate', e.target.value)}
                placeholder="e.g., knows"
                className="w-full bg-[var(--bg-main)] border border-[var(--border)] rounded px-3 py-2 text-xs text-slate-200 focus:outline-none focus:border-[var(--accent-blue)] font-mono"
              />
            </div>
            
            <div>
              <label className="text-[9px] text-[var(--text-muted)] uppercase tracking-wider block mb-1.5 font-medium">
                Object
              </label>
              <input
                type="text"
                value={scanOptions.object || ''}
                onChange={(e) => updateFilter('object', e.target.value)}
                placeholder="e.g., bob"
                className="w-full bg-[var(--bg-main)] border border-[var(--border)] rounded px-3 py-2 text-xs text-slate-200 focus:outline-none focus:border-[var(--accent-blue)] font-mono"
              />
            </div>
            
            <div>
              <label className="text-[9px] text-[var(--text-muted)] uppercase tracking-wider block mb-1.5 font-medium">
                Graph
              </label>
              <input
                type="text"
                value={scanOptions.graph || ''}
                onChange={(e) => updateFilter('graph', e.target.value)}
                placeholder="e.g., doc1"
                className="w-full bg-[var(--bg-main)] border border-[var(--border)] rounded px-3 py-2 text-xs text-slate-200 focus:outline-none focus:border-[var(--accent-blue)] font-mono"
              />
            </div>
            
            <div>
              <label className="text-[9px] text-[var(--text-muted)] uppercase tracking-wider block mb-1.5 font-medium">
                Limit
              </label>
              <input
                type="number"
                value={scanOptions.limit || 100}
                onChange={(e) => updateFilter('limit', e.target.value)}
                className="w-full bg-[var(--bg-main)] border border-[var(--border)] rounded px-3 py-2 text-xs text-slate-200 focus:outline-none focus:border-[var(--accent-blue)] font-mono"
              />
            </div>
          </div>
          
          <button
            onClick={() => runQuery(scanOptions)}
            disabled={isLoading}
            className="w-full py-2 bg-[var(--accent-blue)]/10 hover:bg-[var(--accent-blue)]/20 border border-[var(--accent-blue)]/30 rounded text-[10px] text-[var(--accent-blue)] transition-colors font-black uppercase tracking-wider"
          >
            {isLoading ? (
              <Loader2 size={14} className="animate-spin mx-auto" />
            ) : (
              'Apply Filters'
            )}
          </button>
        </div>
      </div>
      
      {/* Results Table */}
      <div className="flex-1 flex flex-col overflow-hidden">
        <div className="px-4 py-3 border-b border-[var(--border)] flex items-center justify-between bg-[var(--bg-surface)]">
          <span className="text-[10px] text-slate-500 font-medium">
            Showing {results.length} of {total.toLocaleString()} facts
          </span>
        </div>
        
        <div className="flex-1 overflow-auto custom-scrollbar">
          {results.length === 0 ? (
            <div className="flex items-center justify-center h-full text-slate-600 text-xs">
              {isLoading ? (
                <Loader2 size={16} className="animate-spin mr-2" />
              ) : (
                'No facts found'
              )}
            </div>
          ) : (
            <table className="w-full text-left">
              <thead className="sticky top-0 bg-[var(--bg-surface)] border-b border-[var(--border)]">
                <tr>
                  <th className="px-4 py-2.5 text-[9px] font-black uppercase tracking-wider text-slate-600 w-1/4">Subject</th>
                  <th className="px-4 py-2.5 text-[9px] font-black uppercase tracking-wider text-slate-600 w-1/4">Predicate</th>
                  <th className="px-4 py-2.5 text-[9px] font-black uppercase tracking-wider text-slate-600 w-1/4">Object</th>
                  <th className="px-4 py-2.5 text-[9px] font-black uppercase tracking-wider text-slate-600 w-1/4">Graph</th>
                </tr>
              </thead>
              <tbody>
                {results.map((fact, idx) => (
                  <tr
                    key={`${fact.subject}-${fact.predicate}-${fact.object}-${idx}`}
                    onClick={() => setSelectedFact(fact)}
                    className={`border-b border-[var(--border)] cursor-pointer transition-colors ${
                      selectedFact === fact
                        ? 'bg-[var(--accent-blue)]/10'
                        : 'hover:bg-[var(--bg-elevated)]'
                    }`}
                  >
                    <td className="px-4 py-2 text-xs font-mono text-[var(--accent-blue)] truncate max-w-[200px]">
                      {fact.subject}
                    </td>
                    <td className="px-4 py-2 text-xs font-mono text-[var(--accent-purple)] truncate max-w-[200px]">
                      {fact.predicate}
                    </td>
                    <td className="px-4 py-2 text-xs font-mono text-[var(--accent-green)] truncate max-w-[200px]">
                      {fact.object}
                    </td>
                    <td className="px-4 py-2 text-xs font-mono text-slate-500 truncate max-w-[200px]">
                      {fact.graph || '-'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>
      
      {/* Detail Panel */}
      {selectedFact && (
        <div className="w-80 border-l border-[var(--border)] bg-[var(--bg-surface)] flex flex-col">
          <div className="p-4 border-b border-[var(--border)] flex items-center justify-between">
            <h3 className="text-[10px] font-black uppercase tracking-[0.15em] text-slate-500">
              Fact Details
            </h3>
            <button onClick={() => setSelectedFact(null)} className="text-slate-500 hover:text-slate-200">
              <X size={14} />
            </button>
          </div>
          <div className="p-4 space-y-4 overflow-auto custom-scrollbar flex-1">
            <div>
              <label className="text-[9px] text-[var(--text-muted)] uppercase tracking-wider block mb-1.5 font-medium">
                Subject
              </label>
              <div className="text-xs font-mono text-[var(--accent-blue)] break-all bg-[var(--bg-main)] p-2 rounded border border-[var(--border)]">
                {selectedFact.subject}
              </div>
            </div>
            <div>
              <label className="text-[9px] text-[var(--text-muted)] uppercase tracking-wider block mb-1.5 font-medium">
                Predicate
              </label>
              <div className="text-xs font-mono text-[var(--accent-purple)] break-all bg-[var(--bg-main)] p-2 rounded border border-[var(--border)]">
                {selectedFact.predicate}
              </div>
            </div>
            <div>
              <label className="text-[9px] text-[var(--text-muted)] uppercase tracking-wider block mb-1.5 font-medium">
                Object
              </label>
              <div className="text-xs font-mono text-[var(--accent-green)] break-all bg-[var(--bg-main)] p-2 rounded border border-[var(--border)]">
                {selectedFact.object}
              </div>
            </div>
            <div>
              <label className="text-[9px] text-[var(--text-muted)] uppercase tracking-wider block mb-1.5 font-medium">
                Graph
              </label>
              <div className="text-xs font-mono text-slate-500 break-all bg-[var(--bg-main)] p-2 rounded border border-[var(--border)]">
                {selectedFact.graph || '(default)'}
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
