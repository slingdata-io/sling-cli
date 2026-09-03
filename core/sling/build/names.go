package build

import (
	"fmt"
	"sort"
	"strings"
)

// NameRef is a resolved model or seed.
type NameRef struct {
	Name          string
	RelPath       string
	FullTableName string
	Model         *Model
	Seed          *Seed
}

// NameNotFoundError is returned when no model or seed matches.
type NameNotFoundError struct {
	Query       string
	Kind        string // "ref" or "selector"
	Suggestions []NameRef
}

func (e *NameNotFoundError) Error() string {
	var b strings.Builder
	if e.Kind == "selector" {
		fmt.Fprintf(&b, "selector '%s' not found.", e.Query)
	} else {
		fmt.Fprintf(&b, "ref('%s') not found.", e.Query)
	}
	if len(e.Suggestions) == 0 {
		return b.String()
	}
	b.WriteString(" Did you mean:\n")
	for _, c := range e.Suggestions {
		fmt.Fprintf(&b, "  %-20s %s\n", c.Name, c.RelPath)
	}
	return strings.TrimRight(b.String(), "\n")
}

func (e *NameNotFoundError) asSelector() *NameNotFoundError {
	cp := *e
	cp.Kind = "selector"
	return &cp
}

func nameRefFromModel(m *Model) NameRef {
	return NameRef{
		Name:          m.Name,
		RelPath:       m.RelPath,
		FullTableName: m.FullTableName,
		Model:         m,
	}
}

func nameRefFromSeed(s *Seed) NameRef {
	return NameRef{
		Name:          s.Name,
		RelPath:       s.RelPath,
		FullTableName: s.FullTableName,
		Seed:          s,
	}
}

// stripDatabase removes the leading database qualifier from a three-part name.
func stripDatabase(fullName string) string {
	parts := strings.Split(fullName, ".")
	if len(parts) == 3 {
		return parts[1] + "." + parts[2]
	}
	return fullName
}

// ResolveName returns the model or seed for name.
// name may be the file stem ("dim_customers") or the prod table name
// ("marts.dim_customers", or "DB.marts.dim_customers" on three-part projects).
func (p *BuildProject) ResolveName(name string) (*NameRef, error) {
	if name == "" {
		return nil, &NameNotFoundError{Query: name}
	}

	if m, ok := p.Models[name]; ok {
		ref := nameRefFromModel(m)
		return &ref, nil
	}
	if s, ok := p.Seeds[name]; ok {
		ref := nameRefFromSeed(s)
		return &ref, nil
	}

	lower := strings.ToLower(name)
	for _, m := range p.Models {
		if strings.ToLower(m.ProdFullTableName) == lower {
			ref := nameRefFromModel(m)
			return &ref, nil
		}
	}
	for _, s := range p.Seeds {
		if strings.ToLower(s.ProdFullTableName) == lower {
			ref := nameRefFromSeed(s)
			return &ref, nil
		}
	}

	// schema.name spelling on a three-part project
	for _, m := range p.Models {
		if strings.ToLower(stripDatabase(m.ProdFullTableName)) == lower {
			ref := nameRefFromModel(m)
			return &ref, nil
		}
	}
	for _, s := range p.Seeds {
		if strings.ToLower(stripDatabase(s.ProdFullTableName)) == lower {
			ref := nameRefFromSeed(s)
			return &ref, nil
		}
	}

	return nil, &NameNotFoundError{Query: name, Suggestions: p.suggest(name)}
}

func (p *BuildProject) suggest(name string) []NameRef {
	lower := strings.ToLower(name)
	seen := map[string]bool{}
	var out []NameRef
	add := func(r NameRef) {
		if seen[r.Name] {
			return
		}
		seen[r.Name] = true
		out = append(out, r)
	}

	for _, m := range p.Models {
		if strings.HasSuffix(strings.ToLower(m.Name), "_"+lower) {
			add(nameRefFromModel(m))
			continue
		}
		if levenshtein(strings.ToLower(m.Name), lower) <= 2 {
			add(nameRefFromModel(m))
		}
	}
	for _, s := range p.Seeds {
		if strings.HasSuffix(strings.ToLower(s.Name), "_"+lower) {
			add(nameRefFromSeed(s))
			continue
		}
		if levenshtein(strings.ToLower(s.Name), lower) <= 2 {
			add(nameRefFromSeed(s))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	if len(out) > 5 {
		out = out[:5]
	}
	return out
}

func levenshtein(a, b string) int {
	if a == b {
		return 0
	}
	if a == "" {
		return len(b)
	}
	if b == "" {
		return len(a)
	}
	prev := make([]int, len(b)+1)
	curr := make([]int, len(b)+1)
	for j := 0; j <= len(b); j++ {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		curr[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			del := prev[j] + 1
			ins := curr[j-1] + 1
			sub := prev[j-1] + cost
			curr[j] = del
			if ins < curr[j] {
				curr[j] = ins
			}
			if sub < curr[j] {
				curr[j] = sub
			}
		}
		prev, curr = curr, prev
	}
	return prev[len(b)]
}

func selectorResolveError(err error) error {
	switch e := err.(type) {
	case *NameNotFoundError:
		return e.asSelector()
	default:
		return err
	}
}
