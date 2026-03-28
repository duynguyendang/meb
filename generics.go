package meb

import (
	"fmt"
	"iter"
	"reflect"
)

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

func MustValue[T any](f Fact) T {
	v, ok := Value[T](f)
	if !ok {
		panic(fmt.Sprintf("failed to cast %v to %v", f.Object, reflect.TypeOf(*new(T))))
	}
	return v
}

func ValueOrDefault[T any](f Fact, defaultVal T) T {
	if v, ok := Value[T](f); ok {
		return v
	}
	return defaultVal
}

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

func First(seq iter.Seq2[Fact, error]) (Fact, error) {
	for f, err := range seq {
		if err != nil {
			return Fact{}, err
		}
		return f, nil
	}
	return Fact{}, fmt.Errorf("no facts found")
}

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
