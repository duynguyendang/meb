import React from 'react';
import { Database, FileText, Network, Layers, Activity, ChevronRight, HardDrive } from 'lucide-react';

interface StatsPanelProps {
  stats: {
    totalFacts?: number;
    totalDocuments?: number;
    totalVectors?: number;
    graphs?: string[];
    predicates?: string[];
  } | null;
  setViewMode: (mode: 'facts' | 'documents' | 'graph' | 'vectors' | 'search') => void;
}

export function StatsPanel({ stats, setViewMode }: StatsPanelProps) {
  if (!stats) {
    return (
      <div className="flex items-center justify-center h-full text-[var(--text-muted)] text-xs">
        Loading stats...
      </div>
    );
  }
  
  const statCards = [
    {
      label: 'Total Facts',
      value: stats.totalFacts || 0,
      icon: HardDrive,
      color: 'var(--accent-blue)',
      view: 'facts' as const,
    },
    {
      label: 'Documents',
      value: stats.totalDocuments || 0,
      icon: FileText,
      color: 'var(--accent-green)',
      view: 'documents' as const,
    },
    {
      label: 'Vectors',
      value: stats.totalVectors || 0,
      icon: Network,
      color: 'var(--accent-purple)',
      view: 'vectors' as const,
    },
    {
      label: 'Graphs',
      value: stats.graphs?.length || 0,
      icon: Layers,
      color: 'var(--accent-orange)',
      view: 'facts' as const,
    },
  ];
  
  return (
    <div className="p-6 overflow-auto custom-scrollbar h-full">
      <h2 className="text-sm font-bold text-[var(--text-primary)] mb-6">Dashboard</h2>
      
      {/* Stat Cards */}
      <div className="grid grid-cols-4 gap-3 mb-6">
        {statCards.map((card) => (
          <button
            key={card.label}
            onClick={() => setViewMode(card.view)}
            className="p-4 bg-[var(--bg-surface)] border border-[var(--border)] rounded-xl hover:border-[var(--accent-blue)]/50 transition-all group text-left"
          >
            <div className="flex items-center justify-between mb-3">
              <card.icon size={16} style={{ color: card.color }} />
              <ChevronRight size={12} className="text-[var(--text-muted)] group-hover:text-[var(--text-primary)] transition-colors" />
            </div>
            <div className="text-xl font-bold text-[var(--text-primary)] mb-0.5">
              {card.value.toLocaleString()}
            </div>
            <div className="text-[9px] text-[var(--text-muted)] uppercase tracking-wider font-medium">
              {card.label}
            </div>
          </button>
        ))}
      </div>
      
      {/* Graphs & Predicates */}
      <div className="grid grid-cols-2 gap-3">
        {/* Graphs */}
        <div className="p-4 bg-[var(--bg-surface)] border border-[var(--border)] rounded-xl">
          <div className="flex items-center gap-2 mb-3">
            <Layers size={14} className="text-[var(--accent-orange)]" />
            <h3 className="text-xs font-bold text-[var(--text-secondary)]">Graphs</h3>
          </div>
          {stats.graphs && stats.graphs.length > 0 ? (
            <div className="space-y-1.5">
              {stats.graphs.slice(0, 10).map((graph) => (
                <div
                  key={graph}
                  className="px-3 py-2 bg-[var(--bg-main)] rounded text-xs font-mono text-[var(--text-secondary)] truncate border border-[var(--border)]"
                >
                  {graph}
                </div>
              ))}
              {stats.graphs.length > 10 && (
                <div className="text-[9px] text-[var(--text-muted)]">
                  +{stats.graphs.length - 10} more
                </div>
              )}
            </div>
          ) : (
            <div className="text-xs text-[var(--text-muted)]">No graphs</div>
          )}
        </div>
        
        {/* Predicates */}
        <div className="p-4 bg-[var(--bg-surface)] border border-[var(--border)] rounded-xl">
          <div className="flex items-center gap-2 mb-3">
            <Activity size={14} className="text-[var(--accent-blue)]" />
            <h3 className="text-xs font-bold text-[var(--text-secondary)]">Predicates</h3>
          </div>
          {stats.predicates && stats.predicates.length > 0 ? (
            <div className="flex flex-wrap gap-1.5">
              {stats.predicates.slice(0, 20).map((pred) => (
                <span
                  key={pred}
                  className="px-2 py-1 bg-[var(--accent-blue)]/10 text-[var(--accent-blue)] rounded text-[10px] font-mono"
                >
                  {pred}
                </span>
              ))}
              {stats.predicates.length > 20 && (
                <span className="text-[9px] text-[var(--text-muted)]">
                  +{stats.predicates.length - 20} more
                </span>
              )}
            </div>
          ) : (
            <div className="text-xs text-[var(--text-muted)]">No predicates</div>
          )}
        </div>
      </div>
    </div>
  );
}
