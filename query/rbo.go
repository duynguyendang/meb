package query

type PlanType int

const (
	PureLFTJ          PlanType = iota
	PureVectorSearch
	GraphFirst
	VectorFirst
	IntersectionFirst
)

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

	if estimatedSelectivity < 0.2 {
		plan.Type = GraphFirst
		return plan
	}

	plan.Type = VectorFirst
	return plan
}
