import React, { useState, useEffect } from 'react';
import { useMEB } from './hooks/useMEB';
import { Sidebar } from './components/Sidebar';
import { Header } from './components/Header';
import { FactsPanel } from './components/FactsPanel';
import { DocumentsPanel } from './components/DocumentsPanel';
import { VectorSearchPanel } from './components/VectorSearchPanel';
import { StatsPanel } from './components/StatsPanel';
import { SettingsModal } from './components/SettingsModal';

function App() {
  const ctx = useMEB();
  const [showSettings, setShowSettings] = useState(false);
  const [sidebarWidth, setSidebarWidth] = useState(280);
  const [theme, setTheme] = useState<'dark' | 'light'>('dark');

  useEffect(() => {
    const saved = localStorage.getItem('meb-theme') as 'dark' | 'light' | null;
    if (saved) {
      setTheme(saved);
    }
  }, []);

  useEffect(() => {
    if (theme === 'light') {
      document.documentElement.classList.add('light');
    } else {
      document.documentElement.classList.remove('light');
    }
    localStorage.setItem('meb-theme', theme);
  }, [theme]);

  const toggleTheme = () => {
    setTheme(t => t === 'dark' ? 'light' : 'dark');
  };
  
  const renderMainContent = () => {
    switch (ctx.viewMode) {
      case 'facts':
        return <FactsPanel {...ctx} />;
      case 'documents':
        return <DocumentsPanel {...ctx} />;
      case 'vectors':
      case 'search':
        return <VectorSearchPanel {...ctx} />;
      default:
        return <StatsPanel {...ctx} />;
    }
  };
  
  return (
    <div className="flex h-screen w-screen bg-[var(--bg-main)] text-slate-400 overflow-hidden font-sans">
      {/* Sidebar */}
      <aside
        style={{ width: sidebarWidth }}
        className="glass-sidebar flex flex-col shrink-0 relative"
      >
        <Sidebar 
          {...ctx} 
          theme={theme}
          onToggleTheme={toggleTheme}
          onSettingsClick={() => setShowSettings(true)}
        />
        <div
          onMouseDown={(e) => {
            const startX = e.clientX;
            const startWidth = sidebarWidth;
            const onMouseMove = (e: MouseEvent) => {
              const newWidth = startWidth + (e.clientX - startX);
              setSidebarWidth(Math.max(200, Math.min(500, newWidth)));
            };
            const onMouseUp = () => {
              document.removeEventListener('mousemove', onMouseMove);
              document.removeEventListener('mouseup', onMouseUp);
            };
            document.addEventListener('mousemove', onMouseMove);
            document.addEventListener('mouseup', onMouseUp);
          }}
          className="absolute right-0 top-0 bottom-0 w-1.5 cursor-col-resize hover:bg-[var(--accent-teal)]/20 active:bg-[var(--accent-teal)]/50 transition-colors z-40"
        />
      </aside>
      
      {/* Main Content */}
      <div className="flex-1 flex flex-col min-w-0">
        <Header {...ctx} theme={theme} onToggleTheme={toggleTheme} />
        
        <main className="flex-1 overflow-hidden">
          {ctx.error && (
            <div className="bg-[var(--accent-red)]/10 border border-[var(--accent-red)]/30 text-[var(--accent-red)] px-4 py-2 text-xs font-mono">
              <i className="fas fa-exclamation-circle mr-2"></i>
              {ctx.error}
            </div>
          )}
          {renderMainContent()}
        </main>
      </div>
      
      {/* Settings Modal */}
      {showSettings && (
        <SettingsModal 
          apiBase={ctx.apiBase}
          onSave={ctx.setApiBase}
          onClose={() => setShowSettings(false)}
        />
      )}
    </div>
  );
}

export default App;
