package query

type PlanType int

const (
	PureLFTJ          PlanType = iota
	PureVectorSearch
	GraphFirst
	VectorFirst
	IntersectionFirst
)

// DefaultSelectivityThreshold is the default estimated selectivity below which
// GraphFirst is preferred over VectorFirst. Can be tuned per deployment.
var DefaultSelectivityThreshold = 0.2

type ExecutionPlan struct {
	Type        PlanType
	FilterCount int
	HasVector   bool
	IndexType   string
}

func Optimize(hasVector bool, filterCount int, estimatedSelectivity float64) ExecutionPlan {
	plan := ExecutionPlan{
		HasVector:   hasVector,
		FilterCount: filterCount,
	}

	if !hasVector {
		plan.Type = PureLFTJ
		return plan
	}

	if filterCount == 0 {
		plan.Type = PureVectorSearch
		return plan
	}

	if filterCount >= 2 {
		plan.Type = IntersectionFirst
		return plan
	}

	if estimatedSelectivity < DefaultSelectivityThreshold {
		plan.Type = GraphFirst
		return plan
	}

	plan.Type = VectorFirst
	return plan
}
