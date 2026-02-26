package predicates

import (
	"fmt"
	"strings"
)

type Predicate string

func (p Predicate) String() string {
	return string(p)
}

func (p Predicate) Namespace() string {
	parts := strings.SplitN(string(p), ":", 2)
	if len(parts) == 2 {
		return parts[0]
	}
	return ""
}

func (p Predicate) Name() string {
	parts := strings.SplitN(string(p), ":", 2)
	if len(parts) == 2 {
		return parts[1]
	}
	return string(p)
}

func New(namespace, name string) Predicate {
	if namespace == "" {
		return Predicate(name)
	}
	return Predicate(fmt.Sprintf("%s:%s", namespace, name))
}

func MustParse(s string) Predicate {
	return Predicate(s)
}

type Namespace struct {
	prefix string
}

func NS(prefix string) *Namespace {
	return &Namespace{prefix: prefix}
}

func (n *Namespace) P(name string) Predicate {
	if n.prefix == "" {
		return Predicate(name)
	}
	return Predicate(n.prefix + ":" + name)
}
