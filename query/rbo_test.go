package query

import "testing"

func TestOptimizePureLFTJ(t *testing.T) {
	plan := Optimize(false, 0, 0)
	if plan.Type != PureLFTJ {
		t.Errorf("expected PureLFTJ, got %v", plan.Type)
	}
}

func TestOptimizePureVectorSearch(t *testing.T) {
	plan := Optimize(true, 0, 0)
	if plan.Type != PureVectorSearch {
		t.Errorf("expected PureVectorSearch, got %v", plan.Type)
	}
}

func TestOptimizeGraphFirst(t *testing.T) {
	plan := Optimize(true, 1, 0.1)
	if plan.Type != GraphFirst {
		t.Errorf("expected GraphFirst, got %v", plan.Type)
	}
}

func TestOptimizeVectorFirst(t *testing.T) {
	plan := Optimize(true, 1, 0.5)
	if plan.Type != VectorFirst {
		t.Errorf("expected VectorFirst, got %v", plan.Type)
	}
}

func TestOptimizeIntersectionFirst(t *testing.T) {
	plan := Optimize(true, 2, 0.5)
	if plan.Type != IntersectionFirst {
		t.Errorf("expected IntersectionFirst, got %v", plan.Type)
	}
	plan2 := Optimize(true, 3, 0.1)
	if plan2.Type != IntersectionFirst {
		t.Errorf("expected IntersectionFirst for 3 filters, got %v", plan2.Type)
	}
}

func TestOptimizeAllBranches(t *testing.T) {
	tests := []struct {
		name              string
		hasVector         bool
		filterCount       int
		selectivity       float64
		expected          PlanType
	}{
		{"no vector", false, 1, 0.5, PureLFTJ},
		{"pure vector", true, 0, 0, PureVectorSearch},
		{"graph first selective", true, 1, 0.19, GraphFirst},
		{"graph first at boundary", true, 1, 0.0, GraphFirst},
		{"vector first broad", true, 1, 0.2, VectorFirst},
		{"vector first very broad", true, 1, 0.99, VectorFirst},
		{"intersection 2 filters", true, 2, 0.1, IntersectionFirst},
		{"intersection 5 filters", true, 5, 0.5, IntersectionFirst},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := Optimize(tt.hasVector, tt.filterCount, tt.selectivity)
			if plan.Type != tt.expected {
				t.Errorf("Optimize(%v,%d,%.2f) = %v, want %v",
					tt.hasVector, tt.filterCount, tt.selectivity, plan.Type, tt.expected)
			}
			if plan.HasVector != tt.hasVector {
				t.Errorf("HasVector = %v, want %v", plan.HasVector, tt.hasVector)
			}
			if plan.FilterCount != tt.filterCount {
				t.Errorf("FilterCount = %d, want %d", plan.FilterCount, tt.filterCount)
			}
		})
	}
}
