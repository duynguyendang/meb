package query

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/duynguyendang/meb/dict"
)

// Executor represents the top-level executor for Datalog queries
type Executor struct {
	optimizer *QueryOptimizer
	engine    *LFTJEngine
	dict      dict.Dictionary
}

// NewExecutor creates a new query executor
func NewExecutor(optimizer *QueryOptimizer, engine *LFTJEngine, dictionary dict.Dictionary) *Executor {
	return &Executor{
		optimizer: optimizer,
		engine:    engine,
		dict:      dictionary,
	}
}

// ExecuteDatalog parses, optimizes, compiles, and executes a raw Datalog query string.
func (e *Executor) ExecuteDatalog(ctx context.Context, datalogQuery string) ([]map[string]string, error) {
	slog.Debug("executing datalog query", "query", datalogQuery)

	// 1. Optimize and Plan
	plan, err := e.optimizer.OptimizeDatalogQuery(datalogQuery)
	if err != nil {
		return nil, fmt.Errorf("datalog optimization failed: %w", err)
	}

	slog.Debug("datalog plan generated", "cost", plan.Cost, "steps", len(plan.Order))

	// 2. Compile to Leapfrog Triejoin physical operations
	lftjQuery, err := e.optimizer.CompileToLFTJ(plan, e.dict)
	if err != nil {
		return nil, fmt.Errorf("datalog compilation failed: %w", err)
	}

	// 3. Execute
	var stringResults []map[string]string

	for tuple, execErr := range e.engine.Execute(ctx, lftjQuery) {
		if execErr != nil {
			return nil, fmt.Errorf("datalog execution failed: %w", execErr)
		}

		resultMap := make(map[string]string)

		// 4. Decode results from uint64 dictionaries back to strings
		for varName, valID := range tuple {
			// Skip internal anonymous variables
			if strings.HasPrefix(varName, "_") {
				continue
			}

			valStr, dictErr := e.dict.GetString(valID)
			if dictErr != nil {
				// Fallback to literal number if dict misses
				valStr = fmt.Sprintf("ID:%d", valID)
			}
			resultMap[varName] = valStr
		}

		stringResults = append(stringResults, resultMap)
	}

	return stringResults, nil
}
