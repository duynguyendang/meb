import React from 'react';
import { Database, FileText, Network, Search, Settings, ChevronRight, Activity, Layers, HardDrive, Sun, Moon } from 'lucide-react';
import { ViewMode } from '../types';

interface SidebarProps {
  viewMode: ViewMode;
  setViewMode: (mode: ViewMode) => void;
  theme: 'dark' | 'light';
  onToggleTheme: () => void;
  stats: {
    totalFacts?: number;
    totalDocuments?: number;
    totalVectors?: number;
    graphs?: string[];
    predicates?: string[];
  } | null;
  onSettingsClick: () => void;
}

const navItems: { id: ViewMode; label: string; icon: React.ReactNode }[] = [
  { id: 'facts', label: 'Facts', icon: <Database size={14} /> },
  { id: 'documents', label: 'Documents', icon: <FileText size={14} /> },
  { id: 'vectors', label: 'Vectors', icon: <Network size={14} /> },
  { id: 'search', label: 'Search', icon: <Search size={14} /> },
];

export function Sidebar({ viewMode, setViewMode, theme, onToggleTheme, stats, onSettingsClick }: SidebarProps) {
  return (
    <div className="flex flex-col h-full p-4">
      {/* Logo */}
      <div className="flex items-center gap-3 mb-6 px-2">
        <div className="w-9 h-9 rounded bg-[var(--accent-teal)] flex items-center justify-center text-[var(--bg-main)] font-black text-sm shadow-[0_0_15px_rgba(45,212,191,0.4)]">
          M
        </div>
        <div>
          <h1 className="text-sm font-bold text-[var(--text-primary)] tracking-tight">MEB Explorer</h1>
          <p className="text-[9px] text-[var(--accent-teal)] font-mono uppercase tracking-widest">
            Knowledge Graph
          </p>
        </div>
      </div>
      
      {/* Stats */}
      {stats && (
        <div className="mb-6 px-2">
          <h2 className="text-[9px] font-black uppercase tracking-[0.2em] text-[var(--text-muted)] mb-3">
            Statistics
          </h2>
          <div className="space-y-2.5">
            <div className="flex items-center justify-between">
              <span className="flex items-center gap-2 text-xs text-[var(--text-secondary)]">
                <HardDrive size={12} className="text-[var(--accent-blue)]" /> Facts
              </span>
              <span className="text-xs font-mono text-[var(--accent-blue)]">
                {stats.totalFacts?.toLocaleString() || '0'}
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="flex items-center gap-2 text-xs text-[var(--text-secondary)]">
                <FileText size={12} className="text-[var(--accent-green)]" /> Documents
              </span>
              <span className="text-xs font-mono text-[var(--accent-green)]">
                {stats.totalDocuments?.toLocaleString() || '0'}
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="flex items-center gap-2 text-xs text-[var(--text-secondary)]">
                <Network size={12} className="text-[var(--accent-purple)]" /> Vectors
              </span>
              <span className="text-xs font-mono text-[var(--accent-purple)]">
                {stats.totalVectors?.toLocaleString() || '0'}
              </span>
            </div>
          </div>
        </div>
      )}
      
      {/* Navigation */}
      <div className="flex-1">
        <h2 className="text-[9px] font-black uppercase tracking-[0.2em] text-[var(--text-muted)] mb-3 px-2">
          Navigate
        </h2>
        <nav className="space-y-1">
          {navItems.map((item) => (
            <button
              key={item.id}
              onClick={() => setViewMode(item.id)}
              className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-lg text-xs font-medium transition-all ${
                viewMode === item.id
                  ? 'bg-[var(--accent-teal)]/10 text-[var(--accent-teal)] border border-[var(--accent-teal)]/30'
                  : 'text-[var(--text-secondary)] hover:bg-[var(--bg-elevated)] hover:text-[var(--text-primary)] border border-transparent'
              }`}
            >
              {item.icon}
              <span>{item.label}</span>
              {viewMode === item.id && (
                <ChevronRight size={12} className="ml-auto" />
              )}
            </button>
          ))}
        </nav>
      </div>
      
      {/* Graphs */}
      {stats?.graphs && stats.graphs.length > 0 && (
        <div className="mb-4 px-2">
          <h2 className="text-[9px] font-black uppercase tracking-[0.2em] text-[var(--text-muted)] mb-2">
            Graphs
          </h2>
          <div className="flex flex-wrap gap-1">
            {stats.graphs.slice(0, 5).map((graph) => (
              <span
                key={graph}
                className="px-2 py-0.5 text-[10px] bg-[var(--bg-elevated)] text-[var(--text-secondary)] rounded font-mono"
              >
                {graph}
              </span>
            ))}
          </div>
        </div>
      )}
      
      {/* Theme Toggle & Settings */}
      <div className="border-t border-[var(--border)] pt-4 mt-auto space-y-2">
        <button
          onClick={onToggleTheme}
          className="w-full flex items-center gap-3 px-3 py-2.5 rounded-lg text-xs font-medium text-[var(--text-secondary)] hover:bg-[var(--bg-elevated)] hover:text-[var(--text-primary)] transition-all"
        >
          {theme === 'dark' ? <Sun size={14} /> : <Moon size={14} />}
          <span>{theme === 'dark' ? 'Light Mode' : 'Dark Mode'}</span>
        </button>
        
        <button
          onClick={onSettingsClick}
          className="w-full flex items-center gap-3 px-3 py-2.5 rounded-lg text-xs font-medium text-[var(--text-secondary)] hover:bg-[var(--bg-elevated)] hover:text-[var(--text-primary)] transition-all"
        >
          <Settings size={14} />
          <span>Settings</span>
        </button>
      </div>
    </div>
  );
}
