package meb_test

import (
    "context"
    "os"
    "path/filepath"
    "testing"

    "github.com/duynguyendang/meb"
    "github.com/duynguyendang/meb/store"
)

func newTestStoreForPrimitives(t *testing.T) *meb.MEBStore {
    t.Helper()
    segDir := filepath.Join(t.TempDir(), "vectors")
    if err := os.MkdirAll(segDir, 0755); err != nil {
        t.Fatalf("failed to create segment dir: %v", err)
    }
    cfg := &store.Config{
        DataDir:        "",
        DictDir:        "",
        InMemory:       true,
        BlockCacheSize: 1 << 20,
        IndexCacheSize: 1 << 20,
        LRUCacheSize:   100,
        Profile:        "Ingest-Heavy",
        SegmentDir:     segDir,
    }
    s, err := meb.NewMEBStore(cfg)
    if err != nil {
        t.Fatalf("NewMEBStore: %v", err)
    }
    t.Cleanup(func() { s.Close() })
    return s
}

func TestFindSubjectsByObject(t *testing.T) {
    m := newTestStoreForPrimitives(t)
    
    // Add test facts
    m.AddFactBatch([]meb.Fact{
        {Subject: "symbol1", Predicate: "has_name", Object: "foo"},
        {Subject: "symbol2", Predicate: "has_name", Object: "foo"},
        {Subject: "symbol3", Predicate: "has_name", Object: "bar"},
    })
    
    ctx := context.Background()
    
    // Test FindSubjectsByObject
    var subjects []string
    for s := range m.FindSubjectsByObject(ctx, "has_name", "foo") {
        subjects = append(subjects, s)
    }
    
    if len(subjects) != 2 {
        t.Errorf("expected 2 subjects, got %d: %v", len(subjects), subjects)
    }
    
    t.Logf("FindSubjectsByObject works! Found %d subjects: %v", len(subjects), subjects)
}

func TestScanSubjectsByPrefix(t *testing.T) {
    m := newTestStoreForPrimitives(t)
    
    // Add test facts
    m.AddFactBatch([]meb.Fact{
        {Subject: "project/pkg/server/server.go:symbol1", Predicate: "defines", Object: "sym1"},
        {Subject: "project/pkg/server/server.go:symbol2", Predicate: "defines", Object: "sym2"},
        {Subject: "project/pkg/other/other.go:symbol3", Predicate: "defines", Object: "sym3"},
    })
    
    ctx := context.Background()
    
    // Test ScanSubjectsByPrefix
    var subjects []string
    for s := range m.ScanSubjectsByPrefix(ctx, "project/pkg/server/") {
        subjects = append(subjects, s)
    }
    
    if len(subjects) != 2 {
        t.Errorf("expected 2 subjects, got %d: %v", len(subjects), subjects)
    }
    
    t.Logf("ScanSubjectsByPrefix works! Found %d subjects: %v", len(subjects), subjects)
}
