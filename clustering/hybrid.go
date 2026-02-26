package clustering

import (
	"encoding/binary"
	"math"
	"math/rand"
	"sort"

	"github.com/dgraph-io/badger/v4"
	"github.com/duynguyendang/meb/vector"
)

const (
	SemanticDim   = 64
	StructuralDim = 16
	HybridDim     = SemanticDim + StructuralDim
	DefaultAlpha  = 0.75

	MaxClusteringNodes = 1000
	MaxKMeansIters     = 10
)

type HybridVector struct {
	Semantic    [SemanticDim]int8
	Structural  [StructuralDim]int8
	NodeID      uint64
	CommunityID uint64
}

type Cluster struct {
	ID            int
	Centroid      HybridVector
	Members       []HybridVector
	MemberNodeIDs []uint64
	Size          int
}

type KnowledgePackage struct {
	Clusters    []FactCluster
	TotalFacts  int
	QueryTimeMs int64
	Fallback    bool
}

type FactCluster struct {
	ClusterID      int
	Members        []uint64
	CommunityID    uint64
	CentroidVector HybridVector
}

type HybridClustering struct {
	db               *badger.DB
	alpha            float32
	community        map[uint64]uint64
	communityParents map[uint64]uint64
}

func NewHybridClustering(db *badger.DB) *HybridClustering {
	return &HybridClustering{
		db:               db,
		alpha:            DefaultAlpha,
		community:        make(map[uint64]uint64),
		communityParents: make(map[uint64]uint64),
	}
}

func (h *HybridClustering) SetAlpha(alpha float32) {
	if alpha < 0 {
		alpha = 0
	}
	if alpha > 1 {
		alpha = 1
	}
	h.alpha = alpha
}

func (h *HybridClustering) LoadCommunities(graphID string) error {
	graphIDNum := hashString(graphID)
	prefix := []byte{LeidenPrefix}

	return h.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()
			if len(key) < 26 {
				continue
			}

			gID := binary.BigEndian.Uint64(key[1:9])
			if gID != graphIDNum {
				continue
			}

			level := key[9]
			commID := binary.BigEndian.Uint64(key[10:18])
			nodeID := binary.BigEndian.Uint64(key[18:26])

			if level == 1 {
				h.community[nodeID] = commID
			} else if level == 2 {
				h.communityParents[commID] = binary.BigEndian.Uint64(key[10:18])
			}
		}
		return nil
	})
}

func (h *HybridClustering) AugmentVectors(vectors []vector.SearchResult, nodeIDs []uint64) []HybridVector {
	if len(vectors) != len(nodeIDs) {
		return nil
	}

	hybrids := make([]HybridVector, len(vectors))

	for i := range vectors {
		hybrids[i].NodeID = nodeIDs[i]

		if commID, ok := h.community[nodeIDs[i]]; ok {
			hybrids[i].CommunityID = commID
			structural := h.embedCommunityID(commID)
			for j := 0; j < StructuralDim; j++ {
				hybrids[i].Structural[j] = int8(float32(structural[j]) * (1 - h.alpha))
			}
		}

		semantic := QuantizeToInt8(vectors[i].Score)
		for j := 0; j < SemanticDim; j++ {
			hybrids[i].Semantic[j] = int8(float32(semantic) * h.alpha)
		}
	}

	return hybrids
}

func (h *HybridClustering) embedCommunityID(commID uint64) [StructuralDim]int8 {
	var result [StructuralDim]int8
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], commID)
	for i := 0; i < 8; i++ {
		result[i] = int8(buf[i])
	}

	if parentID, ok := h.communityParents[commID]; ok {
		var buf4 [4]byte
		binary.LittleEndian.PutUint32(buf4[:], uint32(parentID))
		for i := 0; i < 4; i++ {
			result[8+i] = int8(buf4[i])
		}
	}

	return result
}

type KMeansEngine struct {
	centroids   []HybridVector
	assignments []int
	distances   []float32
}

func NewKMeansEngine() *KMeansEngine {
	return &KMeansEngine{
		centroids:   make([]HybridVector, 0, 64),
		assignments: make([]int, 0, MaxClusteringNodes),
		distances:   make([]float32, 0, MaxClusteringNodes),
	}
}

func (k *KMeansEngine) Cluster(vectors []HybridVector, numClusters, maxIters int) ([]Cluster, error) {
	if len(vectors) == 0 {
		return nil, nil
	}

	if numClusters > len(vectors) {
		numClusters = len(vectors)
	}

	k.initializeCentroidsPlusPlus(vectors, numClusters)

	k.assignments = make([]int, len(vectors))

	for iter := 0; iter < maxIters; iter++ {
		changed := 0

		for i, vec := range vectors {
			nearest := k.findNearestCentroid(vec)
			if k.assignments[i] != nearest {
				k.assignments[i] = nearest
				changed++
			}
		}

		k.updateCentroids(vectors)

		if changed == 0 {
			break
		}
	}

	return k.buildClusters(vectors), nil
}

func (k *KMeansEngine) initializeCentroidsPlusPlus(vectors []HybridVector, numClusters int) {
	n := len(vectors)
	k.centroids = make([]HybridVector, numClusters)

	randIdx := rand.Intn(n)
	k.centroids[0] = vectors[randIdx]

	distances := make([]float32, n)
	for i := 1; i < numClusters; i++ {
		totalDist := float32(0)

		for j := 0; j < n; j++ {
			minDist := float32(math.MaxFloat32)
			for c := 0; c < i; c++ {
				dist := k.cosineDistance(vectors[j], k.centroids[c])
				if dist < minDist {
					minDist = dist
				}
			}
			distances[j] = minDist * minDist
			totalDist += distances[j]
		}

		randThreshold := rand.Float32() * totalDist
		cumsum := float32(0)
		for j := 0; j < n; j++ {
			cumsum += distances[j]
			if cumsum >= randThreshold {
				k.centroids[i] = vectors[j]
				break
			}
		}
	}
}

func (k *KMeansEngine) findNearestCentroid(vec HybridVector) int {
	bestDist := float32(math.MaxFloat32)
	bestIdx := 0

	for i, centroid := range k.centroids {
		dist := k.cosineDistance(vec, centroid)
		if dist < bestDist {
			bestDist = dist
			bestIdx = i
		}
	}

	return bestIdx
}

func (k *KMeansEngine) updateCentroids(vectors []HybridVector) {
	k.centroids = make([]HybridVector, len(k.centroids))
	counts := make([]int, len(k.centroids))

	for i, vec := range vectors {
		cid := k.assignments[i]
		for j := 0; j < SemanticDim; j++ {
			k.centroids[cid].Semantic[j] += vec.Semantic[j]
		}
		for j := 0; j < StructuralDim; j++ {
			k.centroids[cid].Structural[j] += vec.Structural[j]
		}
		counts[cid]++
	}

	for i := range k.centroids {
		if counts[i] > 0 {
			for j := 0; j < SemanticDim; j++ {
				k.centroids[i].Semantic[j] /= int8(counts[i])
			}
			for j := 0; j < StructuralDim; j++ {
				k.centroids[i].Structural[j] /= int8(counts[i])
			}
		}
	}
}

func (k *KMeansEngine) buildClusters(vectors []HybridVector) []Cluster {
	clusters := make([]Cluster, 0, len(k.centroids))

	for i := range k.centroids {
		cluster := Cluster{
			ID:            i,
			Centroid:      k.centroids[i],
			Members:       make([]HybridVector, 0),
			MemberNodeIDs: make([]uint64, 0),
		}
		clusters = append(clusters, cluster)
	}

	for i, vec := range vectors {
		cid := k.assignments[i]
		clusters[cid].Members = append(clusters[cid].Members, vec)
		clusters[cid].MemberNodeIDs = append(clusters[cid].MemberNodeIDs, vec.NodeID)
	}

	for i := range clusters {
		clusters[i].Size = len(clusters[i].Members)
	}

	sort.Slice(clusters, func(i, j int) bool {
		return clusters[i].Size > clusters[j].Size
	})

	return clusters
}

func (k *KMeansEngine) cosineDistance(a, b HybridVector) float32 {
	var dotSem, dotStr, normASem, normBSem, normAStr, normBStr float32

	for i := 0; i < SemanticDim; i++ {
		dotSem += float32(a.Semantic[i]) * float32(b.Semantic[i])
		normASem += float32(a.Semantic[i]) * float32(a.Semantic[i])
		normBSem += float32(b.Semantic[i]) * float32(b.Semantic[i])
	}

	for i := 0; i < StructuralDim; i++ {
		dotStr += float32(a.Structural[i]) * float32(b.Structural[i])
		normAStr += float32(a.Structural[i]) * float32(a.Structural[i])
		normBStr += float32(b.Structural[i]) * float32(b.Structural[i])
	}

	dot := dotSem + dotStr
	normA := float32(math.Sqrt(float64(normASem))) * float32(math.Sqrt(float64(normAStr)))
	normB := float32(math.Sqrt(float64(normBSem))) * float32(math.Sqrt(float64(normBStr)))

	norm := normA * normB
	if norm == 0 {
		return 0
	}

	return 1 - dot/norm
}

func QuantizeToInt8(score float32) int8 {
	if score > 1 {
		score = 1
	}
	if score < -1 {
		score = -1
	}
	return int8(score * 127)
}

type HybridSearchResult struct {
	NodeID      uint64
	Score       float32
	CommunityID uint64
	ClusterID   int
}

type HybridClusteringResult struct {
	Results      []HybridSearchResult
	Clusters     []Cluster
	KnowledgePkg KnowledgePackage
}

func (h *HybridClustering) ClusterResults(
	vectors []vector.SearchResult,
	nodeIDs []uint64,
	numClusters int,
) (*HybridClusteringResult, error) {
	if len(vectors) == 0 {
		return nil, nil
	}

	if len(vectors) > MaxClusteringNodes {
		vectors = vectors[:MaxClusteringNodes]
		nodeIDs = nodeIDs[:MaxClusteringNodes]
	}

	hybrids := h.AugmentVectors(vectors, nodeIDs)

	kmeans := NewKMeansEngine()
	clusters, err := kmeans.Cluster(hybrids, numClusters, MaxKMeansIters)
	if err != nil {
		return nil, err
	}

	results := make([]HybridSearchResult, len(vectors))
	for i := range vectors {
		results[i] = HybridSearchResult{
			NodeID:      nodeIDs[i],
			Score:       vectors[i].Score,
			ClusterID:   kmeans.assignments[i],
			CommunityID: hybrids[i].CommunityID,
		}
	}

	knowledgePkg := h.buildKnowledgePackage(clusters, vectors)

	return &HybridClusteringResult{
		Results:      results,
		Clusters:     clusters,
		KnowledgePkg: knowledgePkg,
	}, nil
}

func (h *HybridClustering) buildKnowledgePackage(clusters []Cluster, vectors []vector.SearchResult) KnowledgePackage {
	pkg := KnowledgePackage{
		Clusters:   make([]FactCluster, 0, len(clusters)),
		TotalFacts: len(vectors),
		Fallback:   false,
	}

	for i, cluster := range clusters {
		fc := FactCluster{
			ClusterID:      i,
			Members:        cluster.MemberNodeIDs,
			CommunityID:    cluster.Centroid.CommunityID,
			CentroidVector: cluster.Centroid,
		}
		pkg.Clusters = append(pkg.Clusters, fc)
	}

	sort.Slice(pkg.Clusters, func(i, j int) bool {
		return len(pkg.Clusters[i].Members) > len(pkg.Clusters[j].Members)
	})

	return pkg
}

func CosineDistanceInt8(aSem, bSem []int8, aStr, bStr []int8) float32 {
	var dot, normA, normB float32

	for i := range aSem {
		dot += float32(aSem[i]) * float32(bSem[i])
		normA += float32(aSem[i]) * float32(aSem[i])
		normB += float32(bSem[i]) * float32(bSem[i])
	}

	for i := range aStr {
		dot += float32(aStr[i]) * float32(bStr[i])
		normA += float32(aStr[i]) * float32(aStr[i])
		normB += float32(bStr[i]) * float32(bStr[i])
	}

	norm := float32(math.Sqrt(float64(normA))) * float32(math.Sqrt(float64(normB)))
	if norm == 0 {
		return 1
	}

	return 1 - dot/norm
}

func EuclideanDistanceInt8(a, b []int8) float32 {
	var dist float32
	for i := range a {
		d := float32(a[i]) - float32(b[i])
		dist += d * d
	}
	return float32(math.Sqrt(float64(dist)))
}
