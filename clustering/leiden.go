package clustering

import (
	"encoding/binary"
	"log/slog"
	"math"
	"math/rand"
	"sort"

	"github.com/dgraph-io/badger/v4"
)

const (
	MaxIterations     = 10
	ConvergenceDelta  = 0.0001
	StagnationWindow  = 3
	DefaultResolution = 1.0
)

type CommunityDetector struct {
	db         *badger.DB
	csr        *CSRGraph
	resolution float64
	levels     int
	store      interface {
		Scan(s, p, o, g string) interface {
			Iterate(func(f interface{}) bool)
		}
	}
}

type LeidenConfig struct {
	Resolution float64
	Levels     int
}

func DefaultLeidenConfig() *LeidenConfig {
	return &LeidenConfig{
		Resolution: DefaultResolution,
		Levels:     MaxLevels,
	}
}

func NewCommunityDetector(db *badger.DB) *CommunityDetector {
	return &CommunityDetector{
		db:         db,
		resolution: DefaultResolution,
		levels:     MaxLevels,
	}
}

func (cd *CommunityDetector) Detect(graphID string) (*CommunityHierarchy, error) {
	slog.Info("starting community detection", "graph", graphID)

	csr, err := cd.buildCSR(graphID)
	if err != nil {
		return nil, err
	}

	if csr.NodeCount == 0 {
		slog.Info("no nodes to cluster", "graph", graphID)
		return &CommunityHierarchy{GraphID: graphID}, nil
	}

	csr.BuildOffsets()
	csr.LogStats()

	hierarchy := cd.runLeiden(csr)
	hierarchy.GraphID = graphID

	if err := cd.persistCommunities(graphID, hierarchy); err != nil {
		slog.Error("failed to persist communities", "error", err)
		return nil, err
	}

	slog.Info("community detection complete",
		"graph", graphID,
		"levels", hierarchy.NumLevels(),
	)

	return hierarchy, nil
}

func (cd *CommunityDetector) buildCSR(graphID string) (*CSRGraph, error) {
	nodeMap := make(map[uint64]uint32)
	var nodeIDs []uint64

	var edges [][2]uint64

	prefix := []byte{0x20}
	err := cd.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()

			if len(key) < 33 {
				continue
			}

			sID := readUint64(key[1:9])
			pID := readUint64(key[9:17])
			oID := readUint64(key[17:25])

			if pID == 0 {
				continue
			}

			if _, ok := nodeMap[sID]; !ok {
				nodeMap[sID] = uint32(len(nodeIDs))
				nodeIDs = append(nodeIDs, sID)
			}
			if _, ok := nodeMap[oID]; !ok {
				nodeMap[oID] = uint32(len(nodeIDs))
				nodeIDs = append(nodeIDs, oID)
			}

			edges = append(edges, [2]uint64{sID, oID})
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	csr := NewCSRGraph(uint32(len(nodeIDs)))
	copy(csr.NodeIDs, nodeIDs)

	for _, edge := range edges {
		from := nodeMap[edge[0]]
		to := nodeMap[edge[1]]
		if from != to {
			csr.AddEdge(from, to)
		}
	}

	return csr, nil
}

func (cd *CommunityDetector) runLeiden(csr *CSRGraph) *CommunityHierarchy {
	hierarchy := &CommunityHierarchy{}

	communities := cd.localMovingPhase(csr)
	communities = cd.refinementPhase(csr, communities)

	hierarchy.Levels = append(hierarchy.Levels, cd.buildCommunities(csr, communities, 0))

	for level := 1; level < cd.levels; level++ {
		aggregated, aggCommunities := cd.aggregateGraph(csr, communities)
		if aggregated.NodeCount <= 1 {
			break
		}

		refined := cd.localMovingPhase(aggregated)
		refined = cd.refinementPhase(aggregated, refined)

		communities = cd.mapToOriginal(refined, aggCommunities)
		hierarchy.Levels = append(hierarchy.Levels, cd.buildCommunities(csr, communities, uint8(level)))

		csr = aggregated
	}

	return hierarchy
}

func (cd *CommunityDetector) localMovingPhase(csr *CSRGraph) []uint32 {
	n := csr.NodeCount
	communities := make([]uint32, n)
	for i := uint32(0); i < n; i++ {
		communities[i] = i
	}

	prevModularity := calculateModularity(csr, communities)
	stagnationCount := 0

	for iter := 0; iter < MaxIterations; iter++ {
		order := rand.Perm(int(n))

		for _, node := range order {
			bestComm := cd.findBestCommunity(csr, communities, uint32(node))
			if bestComm != communities[uint32(node)] {
				communities[uint32(node)] = bestComm
			}
		}

		currModularity := calculateModularity(csr, communities)
		delta := currModularity - prevModularity

		if delta < ConvergenceDelta {
			stagnationCount++
			if stagnationCount >= StagnationWindow {
				slog.Info("Leiden converged early",
					"iteration", iter+1,
					"modularity", currModularity,
				)
				break
			}
		} else {
			stagnationCount = 0
		}

		prevModularity = currModularity

		if iter > 0 && iter%3 == 0 {
			slog.Debug("Leiden iteration",
				"iteration", iter,
				"modularity", currModularity,
				"delta", delta,
			)
		}
	}

	return communities
}

func (cd *CommunityDetector) findBestCommunity(csr *CSRGraph, communities []uint32, node uint32) uint32 {
	currentComm := communities[node]
	bestComm := currentComm
	bestGain := float64(0)

	neighbors := csr.GetNeighbors(node)
	neighborComms := make(map[uint32]bool)
	for _, n := range neighbors {
		neighborComms[communities[n]] = true
	}
	neighborComms[currentComm] = true

	for comm := range neighborComms {
		gain := cd.calculateGain(csr, communities, node, comm)
		if gain > bestGain {
			bestGain = gain
			bestComm = comm
		}
	}

	return bestComm
}

func (cd *CommunityDetector) calculateGain(csr *CSRGraph, communities []uint32, node, targetComm uint32) float64 {
	communityWeight := make(map[uint32]float64)

	neighbors := csr.GetNeighbors(node)
	offsets := csr.AdjOffsets
	start := offsets[node]

	ki := float64(0)
	for i, n := range neighbors {
		weight := float64(csr.Weights[start+uint32(i)])
		ki += weight
		communityWeight[communities[n]] += weight
	}

	currentComm := communities[node]
	m := csr.TotalWeight()

	currentWeight := communityWeight[currentComm]
	removeGain := currentWeight - (ki*communityWeight[currentComm]/m)*cd.resolution

	addGain := float64(0)
	if targetComm != currentComm {
		addGain = communityWeight[targetComm] - (ki*communityWeight[targetComm]/m)*cd.resolution
	}

	return addGain - removeGain
}

func (cd *CommunityDetector) refinementPhase(csr *CSRGraph, communities []uint32) []uint32 {
	n := csr.NodeCount

	for iter := 0; iter < 3; iter++ {
		order := rand.Perm(int(n))
		changed := false

		for _, node := range order {
			currentComm := communities[uint32(node)]

			neighbors := csr.GetNeighbors(uint32(node))
			var neighborComms []uint32
			for _, n := range neighbors {
				if communities[n] != currentComm {
					neighborComms = append(neighborComms, communities[n])
				}
			}

			if len(neighborComms) == 0 {
				continue
			}

			bestComm := currentComm
			bestSize := int(^uint(0) >> 1)

			for _, comm := range neighborComms {
				size := 0
				for _, c := range communities {
					if c == comm {
						size++
					}
				}
				if size < bestSize {
					bestSize = size
					bestComm = comm
				}
			}

			if bestComm != currentComm && bestSize > 1 {
				communities[uint32(node)] = bestComm
				changed = true
			}
		}

		if !changed {
			break
		}
	}

	return communities
}

func (cd *CommunityDetector) aggregateGraph(csr *CSRGraph, communities []uint32) (*CSRGraph, map[uint32]uint32) {
	commMap := make(map[uint32]uint32)
	var commIDs []uint32

	for _, c := range communities {
		if _, ok := commMap[c]; !ok {
			commMap[c] = uint32(len(commIDs))
			commIDs = append(commIDs, c)
		}
	}

	agg := NewCSRGraph(uint32(len(commIDs)))

	offsets := csr.AdjOffsets
	edges := csr.AdjEdges

	for node := uint32(0); node < csr.NodeCount; node++ {
		start := offsets[node]
		end := offsets[node+1]
		fromComm := commMap[communities[node]]

		for i := start; i < end; i++ {
			toNode := edges[i]
			toComm := commMap[communities[toNode]]

			if fromComm != toComm {
				agg.AddEdge(fromComm, toComm)
			}
		}
	}

	agg.BuildOffsets()

	originalComm := make(map[uint32]uint32)
	for _, c := range communities {
		originalComm[commMap[c]] = c
	}

	return agg, originalComm
}

func (cd *CommunityDetector) mapToOriginal(refined []uint32, aggCommunities map[uint32]uint32) []uint32 {
	result := make([]uint32, len(refined))
	for i, rc := range refined {
		result[i] = aggCommunities[rc]
	}
	return result
}

func (cd *CommunityDetector) buildCommunities(csr *CSRGraph, assignments []uint32, level uint8) []Community {
	commMap := make(map[uint32]*Community)

	for node := uint32(0); node < csr.NodeCount; node++ {
		commID := assignments[node]
		if _, ok := commMap[commID]; !ok {
			commMap[commID] = &Community{
				ID:      uint64(commID),
				Level:   level,
				Members: make([]uint64, 0),
				NodeIDs: make([]uint32, 0),
			}
		}
		comm := commMap[commID]
		comm.Members = append(comm.Members, csr.GetNodeID(node))
		comm.NodeIDs = append(comm.NodeIDs, node)
	}

	var communities []Community
	for _, c := range commMap {
		c.Size = len(c.Members)
		communities = append(communities, *c)
	}

	sort.Slice(communities, func(i, j int) bool {
		return communities[i].Size > communities[j].Size
	})

	return communities
}

func (cd *CommunityDetector) persistCommunities(graphID string, hierarchy *CommunityHierarchy) error {
	graphIDNum := hashString(graphID)

	batch := cd.db.NewWriteBatch()
	defer batch.Cancel()

	for level := 0; level < hierarchy.NumLevels(); level++ {
		communities := hierarchy.Levels[level]
		for _, comm := range communities {
			for _, member := range comm.Members {
				key := EncodeCommunityKey(graphIDNum, uint8(level), comm.ID, member)
				value := []byte{byte(comm.Size)}
				if err := batch.Set(key, value); err != nil {
					return err
				}
			}
		}
	}

	return batch.Flush()
}

func (cd *CommunityDetector) GetCommunityMembers(graphID string, level uint8, commID uint64) ([]uint64, error) {
	graphIDNum := hashString(graphID)
	var members []uint64

	prefix := make([]byte, 18)
	prefix[0] = LeidenPrefix
	binary.BigEndian.PutUint64(prefix[1:9], graphIDNum)
	prefix[9] = level
	binary.BigEndian.PutUint64(prefix[10:18], commID)

	err := cd.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()
			if len(key) >= 26 {
				memberID := binary.BigEndian.Uint64(key[18:26])
				members = append(members, memberID)
			}
		}
		return nil
	})

	return members, err
}

func (cd *CommunityDetector) GetNodeCommunityPath(graphID string, nodeID uint64) ([]uint64, error) {
	graphIDNum := hashString(graphID)
	var path []uint64

	for level := uint8(0); level < uint8(MaxLevels); level++ {
		prefix := make([]byte, 18)
		prefix[0] = LeidenPrefix
		binary.BigEndian.PutUint64(prefix[1:9], graphIDNum)
		prefix[9] = level

		err := cd.db.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()

			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				item := it.Item()
				key := item.Key()
				if len(key) >= 26 {
					memberID := binary.BigEndian.Uint64(key[18:26])
					if memberID == nodeID {
						commID := binary.BigEndian.Uint64(key[10:18])
						path = append(path, commID)
						return nil
					}
				}
			}
			return nil
		})

		if err != nil {
			return nil, err
		}

		if len(path) == 0 && level > 0 {
			break
		}
	}

	return path, nil
}

func calculateModularity(csr *CSRGraph, communities []uint32) float64 {
	n := csr.NodeCount
	m := csr.TotalWeight()
	if m == 0 {
		return 0
	}

	communityWeight := make(map[uint32]float64)
	offsets := csr.AdjOffsets
	edges := csr.AdjEdges
	weights := csr.Weights

	for node := uint32(0); node < n; node++ {
		start := offsets[node]
		end := offsets[node+1]
		fromComm := communities[node]

		for i := start; i < end; i++ {
			toNode := edges[i]
			toComm := communities[toNode]
			weight := float64(weights[i])

			if fromComm == toComm {
				communityWeight[fromComm] += weight
			}
		}
	}

	var q float64
	for _, w := range communityWeight {
		q += w
	}

	q = q / (2 * m)
	q = q - (1 / (2 * m * m))

	return q
}

func readUint64(b []byte) uint64 {
	if len(b) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

func hashString(s string) uint64 {
	var h uint64 = 2166136261
	for i := 0; i < len(s); i++ {
		h = uint64(int64(h*31) ^ int64(s[i]))
	}
	return h
}

func init() {
	rand.Seed(12345)
}

func CosineSimilarity(a, b []float32) float32 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}

	var dot, normA, normB float32
	for i := range a {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}

	denom := float32(math.Sqrt(float64(normA))) * float32(math.Sqrt(float64(normB)))
	if denom == 0 {
		return 0
	}

	return dot / denom
}
