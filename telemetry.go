package meb

import (
	"sync"
	"time"
)

type TelemetryEvent struct {
	Type      string
	Timestamp time.Time
	Data      map[string]any
}

type TelemetrySink interface {
	OnEvent(event TelemetryEvent)
}

type telemetryManager struct {
	mu    sync.RWMutex
	sinks []TelemetrySink
}

func (tm *telemetryManager) Register(sink TelemetrySink) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	for _, s := range tm.sinks {
		if s == sink {
			return
		}
	}
	tm.sinks = append(tm.sinks, sink)
}

func (tm *telemetryManager) Unregister(sink TelemetrySink) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	for i, s := range tm.sinks {
		if s == sink {
			tm.sinks = append(tm.sinks[:i], tm.sinks[i+1:]...)
			return
		}
	}
}

func (tm *telemetryManager) Emit(eventType string, data map[string]any) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	if len(tm.sinks) == 0 {
		return
	}
	event := TelemetryEvent{
		Type:      eventType,
		Timestamp: time.Now(),
		Data:      data,
	}
	for _, sink := range tm.sinks {
		sink.OnEvent(event)
	}
}

func (tm *telemetryManager) SinkCount() int {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return len(tm.sinks)
}
