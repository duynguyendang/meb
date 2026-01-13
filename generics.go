package meb

import (
	"fmt"
	"iter"
)

// === Generic Helper Functions ===
// These functions provide type-safe access to Fact objects using Go generics.

// Value safely casts the Fact object to type T.
// Returns the value and true if successful, zero value and false otherwise.
//
// Example:
//
//	for f, err := range store.Scan("Alice", "knows", "", "") {
//	    if err != nil { panic(err) }
//	    name, ok := Value[string](f)
//	    if ok {
//	        fmt.Printf("Alice knows %s\n", name)
//	    }
//	}
func Value[T any](f Fact) (T, bool) {
	var zero T
	if f.Object == nil {
		return zero, false
	}

	v, ok := f.Object.(T)
	if !ok {
		return zero, false
	}

	return v, true
}

// MustValue casts the Fact object to type T or panics.
// Useful for tests and scripts where you're certain of the type.
//
// Example:
//
//	for f, err := range store.Scan("Alice", "age", "", "") {
//	    if err != nil { panic(err) }
//	    age := MustValue[int](f)
//	    fmt.Printf("Alice is %d years old\n", age)
//	}
func MustValue[T any](f Fact) T {
	v, ok := Value[T](f)
	if !ok {
		panic(fmt.Sprintf("failed to cast %v to %T", f.Object, *new(T)))
	}
	return v
}

// ValueOrDefault casts the Fact object to type T.
// Returns the value if successful, otherwise returns the provided default value.
//
// Example:
//
//	for f, err := range store.Scan("Alice", "age", "", "") {
//	    if err != nil { panic(err) }
//	    age := ValueOrDefault(f, 0)
//	    fmt.Printf("Alice is %d years old\n", age)
//	}
func ValueOrDefault[T any](f Fact, defaultVal T) T {
	if v, ok := Value[T](f); ok {
		return v
	}
	return defaultVal
}

// Collect collects all facts from an iterator into a slice.
// Stops on first error and returns it.
//
// Example:
//
//	facts, err := Collect(store.Scan("Alice", "", "", ""))
//	if err != nil { panic(err) }
//	fmt.Printf("Found %d facts about Alice\n", len(facts))
func Collect(seq iter.Seq2[Fact, error]) ([]Fact, error) {
	facts := make([]Fact, 0)

	for f, err := range seq {
		if err != nil {
			return nil, err
		}
		facts = append(facts, f)
	}

	return facts, nil
}

// Filter creates a new iterator that only yields facts matching the predicate.
//
// Example:
//
//	// Find all people Alice knows who are adults
//	filtered := Filter(
//	    store.Scan("Alice", "knows", "", ""),
//	    func(f Fact) bool {
//	        // Check if this person is an adult
//	        ageFacts, _ := Collect(store.Scan(f.Object.(string), "age", "", ""))
//	        for _, af := range ageFacts {
//	            if age, ok := Value[int](af); ok && age >= 18 {
//	                return true
//	            }
//	        }
//	        return false
//	    },
//	)
func Filter(seq iter.Seq2[Fact, error], pred func(Fact) bool) iter.Seq2[Fact, error] {
	return func(yield func(Fact, error) bool) {
		for f, err := range seq {
			if err != nil {
				yield(Fact{}, err)
				return
			}
			if pred(f) {
				if !yield(f, nil) {
					return
				}
			}
		}
	}
}

// Map transforms each fact using the provided function.
//
// Example:
//
//	mapped := Map(store.Scan("Alice", "knows", "", ""), func(f Fact) (string, error) {
//	    name, ok := Value[string](f)
//	    if !ok {
//	        return "", fmt.Errorf("object is not a string")
//	    }
//	    return name, nil
//	})
//
// Map transforms a sequence of facts into a sequence of values of type T.
func Map[T any](seq iter.Seq2[Fact, error], fn func(Fact) (T, error)) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		for f, err := range seq {
			if err != nil {
				yield(*new(T), err)
				return
			}
			result, err := fn(f)
			if err != nil {
				yield(*new(T), err)
				return
			}
			if !yield(result, nil) {
				return
			}
		}
	}
}

// First returns the first fact from the iterator, or an error if none exists.
//
// Example:
//
//	fact, err := First(store.Scan("Alice", "knows", "", ""))
//	if err != nil { panic(err) }
//	fmt.Printf("First person Alice knows: %v\n", fact.Object)
func First(seq iter.Seq2[Fact, error]) (Fact, error) {
	for f, err := range seq {
		if err != nil {
			return Fact{}, err
		}
		return f, nil
	}
	return Fact{}, fmt.Errorf("no facts found")
}

// CountFacts counts the number of facts in the iterator.
// Note: Renamed from Count to avoid conflict with MEBStore.Count() method.
//
// Example:
//
//	count, err := CountFacts(store.Scan("Alice", "", "", ""))
//	if err != nil { panic(err) }
//	fmt.Printf("Alice has %d facts\n", count)
func CountFacts(seq iter.Seq2[Fact, error]) (int, error) {
	count := 0
	for _, err := range seq {
		if err != nil {
			return 0, err
		}
		count++
	}
	return count, nil
}
