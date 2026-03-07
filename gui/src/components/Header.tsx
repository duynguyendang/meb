import React from 'react';
import { RefreshCw, Loader2 } from 'lucide-react';

interface HeaderProps {
  viewMode: string;
  isLoading: boolean;
  loadStats: () => void;
  apiBase: string;
  theme: 'dark' | 'light';
  onToggleTheme: () => void;
  stats: {
    totalFacts?: number;
    totalDocuments?: number;
    totalVectors?: number;
    graphs?: string[];
    predicates?: string[];
  } | null;
}

export function Header({ viewMode, isLoading, loadStats, apiBase, theme, onToggleTheme }: HeaderProps) {
  const getViewTitle = () => {
    switch (viewMode) {
      case 'facts': return 'Facts Browser';
      case 'documents': return 'Document Store';
      case 'vectors': return 'Vector Search';
      case 'search': return 'Semantic Search';
      default: return 'Dashboard';
    }
  };
  
  return (
    <header className="h-12 border-b border-[var(--border)] flex items-center px-6 gap-6 bg-[var(--bg-main)]/90 backdrop-blur-md z-20 shrink-0">
      <div className="flex items-center gap-3">
        <div>
          <h1 className="text-sm font-bold text-[var(--text-primary)]">{getViewTitle()}</h1>
          <p className="text-[9px] text-[var(--text-muted)] font-mono tracking-wider">
            {apiBase.replace('http://', '').replace('https://', '')}
          </p>
        </div>
      </div>
      
      <div className="flex-1" />
      
      <button
        onClick={loadStats}
        disabled={isLoading}
        className="flex items-center gap-2 px-3 py-1.5 bg-[var(--bg-surface)] hover:bg-[var(--bg-elevated)] border border-[var(--border)] rounded text-[10px] text-[var(--text-secondary)] hover:text-[var(--text-primary)] transition-colors font-medium"
      >
        <RefreshCw size={12} className={isLoading ? 'animate-spin' : ''} />
        Refresh
      </button>
    </header>
  );
}
