package clustering

import (
	"encoding/binary"
	"log/slog"
)

type CSRGraph struct {
	AdjEdges   []uint32
	AdjOffsets []uint32
	NodeCount  uint32
	EdgeCount  uint64
	Weights    []float32
	NodeIDs    []uint64
}

func NewCSRGraph(nodeCount uint32) *CSRGraph {
	initialCapacity := nodeCount * 4
	return &CSRGraph{
		AdjEdges:   make([]uint32, 0, initialCapacity),
		AdjOffsets: make([]uint32, nodeCount+1),
		NodeCount:  nodeCount,
		NodeIDs:    make([]uint64, nodeCount),
	}
}

func (g *CSRGraph) AddEdge(from, to uint32) {
	g.AdjEdges = append(g.AdjEdges, to)
	if g.Weights == nil {
		g.Weights = append(g.Weights, 1.0)
	} else {
		g.Weights = append(g.Weights, 1.0)
	}
	g.EdgeCount++
}

func (g *CSRGraph) BuildOffsets() {
	for i := uint32(0); i < g.NodeCount; i++ {
		g.AdjOffsets[i+1] = g.AdjOffsets[i] + g.GetNeighborCount(i)
	}
}

func (g *CSRGraph) GetNeighborCount(node uint32) uint32 {
	if node >= g.NodeCount {
		return 0
	}
	return g.AdjOffsets[node+1] - g.AdjOffsets[node]
}

func (g *CSRGraph) GetNeighbors(node uint32) []uint32 {
	if node >= g.NodeCount {
		return nil
	}
	start := g.AdjOffsets[node]
	end := g.AdjOffsets[node+1]
	return g.AdjEdges[start:end]
}

func (g *CSRGraph) GetNodeID(node uint32) uint64 {
	if node >= g.NodeCount {
		return 0
	}
	return g.NodeIDs[node]
}

func (g *CSRGraph) GetNodeIndex(nodeID uint64) uint32 {
	for i, id := range g.NodeIDs {
		if id == nodeID {
			return uint32(i)
		}
	}
	return ^uint32(0)
}

func (g *CSRGraph) GetWeight(edgeIndex int) float32 {
	if edgeIndex < 0 || edgeIndex >= len(g.Weights) {
		return 1.0
	}
	return g.Weights[edgeIndex]
}

func (g *CSRGraph) TotalWeight() float64 {
	var total float64
	for _, w := range g.Weights {
		total += float64(w)
	}
	return total
}

func (g *CSRGraph) LogStats() {
	slog.Debug("CSR Graph stats",
		"nodes", g.NodeCount,
		"edges", g.EdgeCount,
	)
}

type Community struct {
	ID       uint64
	Members  []uint64
	Size     int
	NodeIDs  []uint32
	Level    uint8
	ParentID uint64
}

type CommunityHierarchy struct {
	GraphID string
	Levels  [][]Community
}

func (h *CommunityHierarchy) GetLevel(level uint8) []Community {
	if int(level) >= len(h.Levels) {
		return nil
	}
	return h.Levels[level]
}

func (h *CommunityHierarchy) GetCommunity(level uint8, commID uint64) *Community {
	levelComms := h.GetLevel(level)
	if levelComms == nil {
		return nil
	}
	for i := range levelComms {
		if levelComms[i].ID == commID {
			return &levelComms[i]
		}
	}
	return nil
}

func (h *CommunityHierarchy) NumLevels() int {
	return len(h.Levels)
}

const (
	LeidenPrefix = 0x30
	MaxLevels    = 3
)

func EncodeCommunityKey(graphID uint64, level uint8, communityID, subjectID uint64) []byte {
	key := make([]byte, 26)
	key[0] = LeidenPrefix
	binary.BigEndian.PutUint64(key[1:9], graphID)
	key[9] = level
	binary.BigEndian.PutUint64(key[10:18], communityID)
	binary.BigEndian.PutUint64(key[18:26], subjectID)
	return key
}

func DecodeCommunityKey(key []byte) (graphID uint64, level uint8, communityID, subjectID uint64) {
	if len(key) < 26 {
		return 0, 0, 0, 0
	}
	graphID = binary.BigEndian.Uint64(key[1:9])
	level = key[9]
	communityID = binary.BigEndian.Uint64(key[10:18])
	subjectID = binary.BigEndian.Uint64(key[18:26])
	return
}
