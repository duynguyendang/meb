import React, { useState } from 'react';
import { X, Save } from 'lucide-react';

interface SettingsModalProps {
  apiBase: string;
  onSave: (url: string) => void;
  onClose: () => void;
}

export function SettingsModal({ apiBase, onSave, onClose }: SettingsModalProps) {
  const [url, setUrl] = useState(apiBase);
  
  const handleSave = () => {
    onSave(url);
    onClose();
  };
  
  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
      <div className="bg-[var(--bg-surface)] border border-[var(--border)] rounded-xl w-96 shadow-2xl">
        <div className="flex items-center justify-between p-4 border-b border-[var(--border)]">
          <h3 className="text-sm font-bold text-white">Settings</h3>
          <button
            onClick={onClose}
            className="text-[var(--text-muted)] hover:text-[var(--text-primary)]"
          >
            <X size={16} />
          </button>
        </div>
        
        <div className="p-4 space-y-4">
          <div>
            <label className="text-xs text-[var(--text-muted)] uppercase tracking-wider block mb-2">
              API Base URL
            </label>
            <input
              type="text"
              value={url}
              onChange={(e) => setUrl(e.target.value)}
              placeholder="http://localhost:8080/v1"
              className="w-full bg-[var(--bg-main)] border border-[var(--border)] rounded px-3 py-2 text-sm text-[var(--text-primary)] focus:outline-none focus:border-[var(--accent-blue)] font-mono"
            />
            <p className="text-[10px] text-[var(--text-muted)] mt-2">
              The base URL for the MEB API server
            </p>
          </div>
        </div>
        
        <div className="flex justify-end gap-2 p-4 border-t border-[var(--border)]">
          <button
            onClick={onClose}
            className="px-4 py-2 text-sm text-[var(--text-secondary)] hover:text-[var(--text-primary)] transition-colors"
          >
            Cancel
          </button>
          <button
            onClick={handleSave}
            className="flex items-center gap-2 px-4 py-2 bg-[var(--accent-blue)] hover:bg-[var(--accent-blue)]/80 rounded text-sm text-white font-medium transition-colors"
          >
            <Save size={14} />
            Save
          </button>
        </div>
      </div>
    </div>
  );
}
