package secrets

import (
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// bareSchemes are vendor-native reference forms that work without `ref+`.
var bareSchemes = []string{"op", "keeper"}

// nativeForms are the identifiers that secret manager consoles show. They
// work as whole values, with no `ref+<kind>://`. Params and a pointer can follow.
var nativeForms = []struct {
	kind string
	re   *regexp.Regexp
}{
	{"azurekeyvault", regexp.MustCompile(`(?i)^https://[a-z0-9-]+\.vault\.(azure\.net|azure\.cn|usgovcloudapi\.net)(:443)?/secrets/[a-z0-9-]+(/[a-z0-9]{32})?/?$`)},
	{"awssecrets", regexp.MustCompile(`^arn:aws[a-z-]*:secretsmanager:[a-z0-9-]+:\d{12}:secret:\S+$`)},
	{"awsssm", regexp.MustCompile(`^arn:aws[a-z-]*:ssm:[a-z0-9-]+:\d{12}:parameter/\S+$`)},
	{"gcpsecrets", regexp.MustCompile(`^(//secretmanager\.googleapis\.com/)?projects/[^/\s]+(/locations/[^/\s]+)?/secrets/[^/\s]+(/versions/[^/\s]+)?$`)},
	{"conjur", regexp.MustCompile(`^[\w.-]+:variable:\S+$`)},
}

// nativeKind returns the backend kind of a native identifier, or "".
func nativeKind(s string) string {
	base, _, _ := strings.Cut(s, "#")
	base, _, _ = strings.Cut(base, "?")
	for _, f := range nativeForms {
		if f.re.MatchString(base) {
			return f.kind
		}
	}
	return ""
}

var kindRe = regexp.MustCompile(`^[a-z0-9_]+$`)

// embeddedRefRe matches a `ref+<kind>://<path>+` reference inside a longer
// string (vals grammar). The closing `+` is optional at the end of the string.
var embeddedRefRe = regexp.MustCompile(`ref\+[a-z0-9_]+://[^+\s]+\+?`)

// Ref is one parsed secret reference.
type Ref struct {
	Raw      string     // original text, used in errors
	Kind     string     // registry kind after alias mapping, e.g. "op", "awssecrets"
	Path     string     // "prod/postgres"
	Params   url.Values // backend params, common params removed
	Pointer  string     // "/password", "" for the whole value
	Provider string     // instance name from ?provider=, "" for default
	Trim     bool
}

// IsRef is true when the whole string is a secret reference.
func IsRef(s string) bool {
	s = strings.TrimSpace(s)
	if strings.Contains(s, "\n") {
		return false
	}
	if nativeKind(s) != "" {
		return true
	}
	for _, scheme := range bareSchemes {
		if rest, ok := strings.CutPrefix(s, scheme+"://"); ok {
			return rest != ""
		}
	}
	rest, ok := strings.CutPrefix(s, "ref+")
	if !ok {
		return false
	}
	rest = strings.TrimSuffix(rest, "+")
	kind, path, ok := strings.Cut(rest, "://")
	return ok && kindRe.MatchString(kind) && path != "" && !strings.Contains(path, "+")
}

// ContainsRef is true when s is a reference or holds an embedded `ref+...+` one.
func ContainsRef(s string) bool {
	return IsRef(s) || embeddedRefRe.MatchString(s)
}

// ParseRef parses a whole-value reference.
func ParseRef(s string) (Ref, error) {
	raw := strings.TrimSpace(s)
	ref := Ref{Raw: raw, Trim: true}
	if !IsRef(raw) {
		return ref, fmt.Errorf("%q is not a secret reference", raw)
	}

	body := raw
	if kind := nativeKind(raw); kind != "" {
		body = "ref+" + kind + "://" + raw
	}
	for _, scheme := range bareSchemes {
		if strings.HasPrefix(body, scheme+"://") {
			body = "ref+" + body
			break
		}
	}
	body = strings.TrimSuffix(strings.TrimPrefix(body, "ref+"), "+")
	scheme, rest, _ := strings.Cut(body, "://")

	kind, ok := lookupKind(scheme)
	if !ok {
		return ref, fmt.Errorf("unknown secret backend %q in reference %q", scheme, raw)
	}
	ref.Kind = kind

	rest, pointer, hasPointer := strings.Cut(rest, "#")
	if hasPointer && pointer != "" {
		if !strings.HasPrefix(pointer, "/") {
			pointer = "/" + pointer
		}
		ref.Pointer = pointer
	}

	path, rawQuery, _ := strings.Cut(rest, "?")
	ref.Path = path
	params, err := url.ParseQuery(rawQuery)
	if err != nil {
		return ref, fmt.Errorf("invalid parameters in reference %q: %w", raw, err)
	}
	if v, ok := params["provider"]; ok {
		ref.Provider = v[0]
		params.Del("provider")
	}
	if v, ok := params["trim"]; ok {
		ref.Trim = !strings.EqualFold(v[0], "false")
		params.Del("trim")
	}
	ref.Params = params
	if ref.Path == "" {
		return ref, fmt.Errorf("reference %q has no path", raw)
	}
	return ref, nil
}

// Key is the cache key: kind + provider + path + params, without the pointer.
// Two fields that read different keys of one secret share one fetch.
func (r Ref) Key() string {
	keys := make([]string, 0, len(r.Params))
	for k := range r.Params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	sb.WriteString(r.Kind + "|" + r.Provider + "|" + r.Path)
	for _, k := range keys {
		for _, v := range r.Params[k] {
			sb.WriteString("|" + k + "=" + v)
		}
	}
	return sb.String()
}

// String is the reference without the pointer, in the `ref+` form.
func (r Ref) String() string {
	s := "ref+" + r.Kind + "://" + r.Path
	if q := r.Params.Encode(); q != "" {
		s += "?" + q
	}
	return s
}

// Select applies the pointer to a JSON or YAML value. Without a pointer it
// returns the value as a string.
func (r Ref) Select(raw []byte) (any, error) {
	if r.Pointer == "" {
		return string(raw), nil
	}
	doc, err := parseDocument(raw)
	if err != nil {
		return nil, fmt.Errorf("secret is not JSON or YAML, so pointer %q cannot apply", r.Pointer)
	}
	cur := doc
	for _, token := range strings.Split(strings.TrimPrefix(r.Pointer, "/"), "/") {
		token = strings.ReplaceAll(strings.ReplaceAll(token, "~1", "/"), "~0", "~")
		switch node := cur.(type) {
		case map[string]any:
			next, ok := node[token]
			if !ok {
				return nil, fmt.Errorf("key %q not found at pointer %q", token, r.Pointer)
			}
			cur = next
		case []any:
			i, err := strconv.Atoi(token)
			if err != nil || i < 0 || i >= len(node) {
				return nil, fmt.Errorf("index %q not found at pointer %q", token, r.Pointer)
			}
			cur = node[i]
		default:
			return nil, fmt.Errorf("pointer %q goes past a scalar value", r.Pointer)
		}
	}
	return cur, nil
}

// parseDocument decodes JSON, else YAML, into maps with string keys.
func parseDocument(raw []byte) (any, error) {
	var doc any
	if err := json.Unmarshal(raw, &doc); err == nil {
		return doc, nil
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	return normalize(doc), nil
}

// normalize turns map[any]any into map[string]any, deeply.
func normalize(v any) any {
	switch t := v.(type) {
	case map[string]any:
		for k, item := range t {
			t[k] = normalize(item)
		}
		return t
	case map[any]any:
		out := make(map[string]any, len(t))
		for k, item := range t {
			out[fmt.Sprint(k)] = normalize(item)
		}
		return out
	case []any:
		for i, item := range t {
			t[i] = normalize(item)
		}
		return t
	default:
		return v
	}
}
