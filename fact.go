package meb

import "fmt"

type Fact struct {
	Subject   string
	Predicate string
	Object    any
}

func (f Fact) String() string {
	return fmt.Sprintf("<%s, %s, %v>", f.Subject, f.Predicate, f.Object)
}

func (f Fact) IsValid() bool {
	return f.Subject != "" && f.Predicate != "" && f.Object != nil
}

func NewFact(subject, predicate string, object any) Fact {
	return Fact{
		Subject:   subject,
		Predicate: predicate,
		Object:    object,
	}
}
