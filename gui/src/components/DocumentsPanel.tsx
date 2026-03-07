import React, { useEffect } from 'react';
import { Loader2, FileText, X } from 'lucide-react';
import { DocumentInfo } from '../types';
import { fetchDocument } from '../services/api';

interface DocumentsPanelProps {
  documents: DocumentInfo[];
  selectedDocument: DocumentInfo | null;
  setSelectedDocument: (doc: DocumentInfo | null) => void;
  isLoading: boolean;
  loadDocuments: () => void;
}

export function DocumentsPanel({
  documents,
  selectedDocument,
  setSelectedDocument,
  isLoading,
  loadDocuments
}: DocumentsPanelProps) {
  useEffect(() => {
    loadDocuments();
  }, [loadDocuments]);
  
  const [content, setContent] = React.useState<string>('');
  const [contentLoading, setContentLoading] = React.useState(false);
  
  useEffect(() => {
    if (selectedDocument) {
      setContentLoading(true);
      fetchDocument(selectedDocument.id)
        .then(doc => {
          setContent(doc.content || '');
        })
        .catch(e => console.error('Failed to load content:', e))
        .finally(() => setContentLoading(false));
    }
  }, [selectedDocument]);
  
  return (
    <div className="flex h-full">
      {/* Document List */}
      <div className="w-80 border-r border-[var(--border)] bg-[var(--bg-surface)] flex flex-col">
        <div className="p-4 border-b border-[var(--border)]">
          <h3 className="text-[10px] font-black uppercase tracking-[0.15em] text-slate-500">
            Documents ({documents.length})
          </h3>
        </div>
        
        <div className="flex-1 overflow-auto custom-scrollbar">
          {documents.length === 0 ? (
            <div className="flex items-center justify-center h-full text-slate-600 text-xs">
              {isLoading ? (
                <Loader2 size={16} className="animate-spin mr-2" />
              ) : (
                'No documents'
              )}
            </div>
          ) : (
            <div className="divide-y divide-[var(--border)]">
              {documents.map((doc) => (
                <button
                  key={doc.id}
                  onClick={() => setSelectedDocument(doc)}
                  className={`w-full text-left px-4 py-3 transition-colors ${
                    selectedDocument?.id === doc.id
                      ? 'bg-[var(--accent-blue)]/10'
                      : 'hover:bg-[var(--bg-elevated)]'
                  }`}
                >
                  <div className="flex items-start gap-3">
                    <FileText size={14} className="text-[var(--accent-green)] mt-0.5 shrink-0" />
                    <div className="min-w-0">
                      <div className="text-xs text-slate-200 truncate font-mono">
                        {doc.id}
                      </div>
                      {doc.metadata && (
                        <div className="text-[10px] text-slate-500 mt-1">
                          {Object.keys(doc.metadata).length} metadata fields
                        </div>
                      )}
                    </div>
                  </div>
                </button>
              ))}
            </div>
          )}
        </div>
      </div>
      
      {/* Document Content */}
      <div className="flex-1 flex flex-col overflow-hidden">
        {selectedDocument ? (
          <>
            <div className="p-3 border-b border-[var(--border)] flex items-center justify-between bg-[var(--bg-surface)]">
              <h3 className="text-xs font-medium text-slate-200 font-mono truncate">
                {selectedDocument.id}
              </h3>
              <button 
                onClick={() => setSelectedDocument(null)}
                className="text-slate-500 hover:text-slate-200"
              >
                <X size={14} />
              </button>
            </div>
            
            <div className="flex-1 overflow-auto custom-scrollbar p-4">
              {contentLoading ? (
                <div className="flex items-center justify-center h-full">
                  <Loader2 size={16} className="animate-spin text-slate-500" />
                </div>
              ) : content ? (
                <pre className="text-xs font-mono text-slate-400 whitespace-pre-wrap break-all">
                  {content}
                </pre>
              ) : (
                <div className="text-slate-600 text-xs">
                  No content available
                </div>
              )}
            </div>
          </>
        ) : (
          <div className="flex items-center justify-center h-full text-slate-600 text-xs">
            Select a document to view
          </div>
        )}
      </div>
    </div>
  );
}
