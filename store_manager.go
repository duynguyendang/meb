package meb

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru/v2"

	"github.com/duynguyendang/meb/store"
)

type MemoryProfile string

const (
	MemoryProfileDefault MemoryProfile = "default"
	MemoryProfileLow     MemoryProfile = "low"
	MaxOpenStores                      = 10
	ProjectListTTL                     = 1 * time.Minute
)

type ProjectMetadata struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

type StoreManager struct {
	baseDir    string
	projects   *lru.Cache[string, *MEBStore]
	mu         sync.RWMutex
	profile    MemoryProfile
	readOnly   bool
	cachedList []ProjectMetadata
	lastList   time.Time
}

func NewStoreManager(baseDir string, profile MemoryProfile, readOnly bool) *StoreManager {
	cache, _ := lru.NewWithEvict[string, *MEBStore](MaxOpenStores, func(key string, value *MEBStore) {
		_ = value.Close()
	})

	return &StoreManager{
		baseDir:  baseDir,
		projects: cache,
		profile:  profile,
		readOnly: readOnly,
	}
}

func (sm *StoreManager) GetStore(projectID string) (*MEBStore, error) {
	if s, ok := sm.projects.Get(projectID); ok {
		return s, nil
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	if s, ok := sm.projects.Get(projectID); ok {
		return s, nil
	}

	projectDir := filepath.Join(sm.baseDir, projectID)
	if _, err := os.Stat(projectDir); os.IsNotExist(err) {
		return nil, fmt.Errorf("project not found: %s", projectID)
	}

	cfg := store.DefaultConfig(projectDir)
	cfg.ReadOnly = sm.readOnly

	if sm.profile == MemoryProfileLow {
		cfg.BlockCacheSize = 64 << 20
		cfg.IndexCacheSize = 64 << 20
		cfg.Profile = "low"
	} else {
		cfg.BlockCacheSize = 128 << 20
		cfg.IndexCacheSize = 128 << 20
		cfg.Profile = "default"
	}

	s, err := NewMEBStore(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to open store for project %s: %w", projectID, err)
	}

	sm.projects.Add(projectID, s)
	return s, nil
}

func (sm *StoreManager) ListProjects() ([]ProjectMetadata, error) {
	sm.mu.RLock()
	if time.Since(sm.lastList) < ProjectListTTL && sm.cachedList != nil {
		list := make([]ProjectMetadata, len(sm.cachedList))
		copy(list, sm.cachedList)
		sm.mu.RUnlock()
		return list, nil
	}
	sm.mu.RUnlock()

	sm.mu.Lock()
	defer sm.mu.Unlock()

	if time.Since(sm.lastList) < ProjectListTTL && sm.cachedList != nil {
		list := make([]ProjectMetadata, len(sm.cachedList))
		copy(list, sm.cachedList)
		return list, nil
	}

	entries, err := os.ReadDir(sm.baseDir)
	if err != nil {
		return nil, err
	}

	var projects []ProjectMetadata
	for _, entry := range entries {
		if entry.IsDir() {
			id := entry.Name()
			meta := ProjectMetadata{
				ID:   id,
				Name: id,
			}

			metaPath := filepath.Join(sm.baseDir, id, "metadata.json")
			if data, err := os.ReadFile(metaPath); err == nil {
				var jsonMeta ProjectMetadata
				if err := json.Unmarshal(data, &jsonMeta); err == nil {
					if jsonMeta.Name != "" {
						meta.Name = jsonMeta.Name
					}
					meta.Description = jsonMeta.Description
				}
			}
			projects = append(projects, meta)
		}
	}

	sm.cachedList = projects
	sm.lastList = time.Now()

	return projects, nil
}

func (sm *StoreManager) CloseAll() {
	sm.projects.Purge()
}
