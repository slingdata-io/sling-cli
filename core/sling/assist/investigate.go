// Investigate surface: error signatures, snapshots, local exec picker, hints, sensitivity.

package assist

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
)

// AlgorithmVersion labels the normalizer generation (not a material prefix).
const AlgorithmVersion = "v1"

// Material prefixes distinguish pattern vs edge digests (composite signature).
const (
	PatternMaterialPrefix = "v1p" // skeleton-only
	EdgeMaterialPrefix    = "v1e" // source|target|skeleton
)

// PartIDLen is hex chars kept for each of pattern and edge digests.
const PartIDLen = 8

// CompositeIDLen is PatternID + EdgeID (16 hex chars, no separator).
const CompositeIDLen = PartIDLen * 2

// SignatureIDLen is an alias for PartIDLen.
const SignatureIDLen = PartIDLen

// SignMeta holds connector types only (never hostnames, DB names, or task type).
// Task type (db-db, file-db, …) is not stored: it is fully determined by the
// Kind of SourceType and TargetType (see InferredTaskType).
type SignMeta struct {
	SourceType dbio.Type // e.g. dbio.TypeDbPrometheus
	TargetType dbio.Type // e.g. dbio.TypeDbPostgres
}

func MakeSignMeta() (sm SignMeta) {
	taskMap, _ := g.UnmarshalMap(cast.ToString(env.TelMap["task"]))
	src := cast.ToString(taskMap["source_type"])
	tgt := cast.ToString(taskMap["target_type"])
	if src == "" {
		src = cast.ToString(env.TelMap["conn_type"])
	}
	return SignMeta{SourceType: dbio.Type(src), TargetType: dbio.Type(tgt)}
}

// Signature is the full result of SignError (composite: pattern + edge).
//
//	PatternID  = hex(sha256("v1p|" + skeleton))[0:8]
//	EdgeID     = hex(sha256("v1e|" + src + "|" + tgt + "|" + skeleton))[0:8]
//	ID         = PatternID + EdgeID   // 16 hex, machine form
//	IDDashed   = PatternID + "-" + EdgeID
//
// Same skeleton ⇒ same PatternID across all source/target pairs.
// Same skeleton + same types ⇒ same EdgeID and ID.
type Signature struct {
	ID              string // 16 hex chars (pattern||edge)
	PatternID       string // 8 hex — skeleton only
	EdgeID          string // 8 hex — types + skeleton
	Skeleton        string // normalized message body
	PatternMaterial string // v1p|<skeleton>
	EdgeMaterial    string // v1e|<source>|<target>|<skeleton>
	ShortLabel      string // human-only slug (not hashed)
	Algorithm       string // e.g. v1
	Meta            SignMeta
}

// SignError normalizes errText + meta into a stable composite signature.
// Pure function: no I/O, deterministic across machines.
func SignError(errText string, meta SignMeta) Signature {
	meta = normalizeMeta(meta)
	skel := Skeleton(errText)
	pMat := PatternMaterial(skel)
	eMat := EdgeMaterial(skel, meta)
	patternID := HashPart(pMat)
	edgeID := HashPart(eMat)
	return Signature{
		ID:              patternID + edgeID,
		PatternID:       patternID,
		EdgeID:          edgeID,
		Skeleton:        skel,
		PatternMaterial: pMat,
		EdgeMaterial:    eMat,
		ShortLabel:      ShortLabel(skel),
		Algorithm:       AlgorithmVersion,
		Meta:            meta,
	}
}

// IDDashed returns pattern-edge with a hyphen for humans / copy-paste.
func (s Signature) IDDashed() string {
	if s.PatternID == "" || s.EdgeID == "" {
		if len(s.ID) == CompositeIDLen {
			return s.ID[:PartIDLen] + "-" + s.ID[PartIDLen:]
		}
		return s.ID
	}
	return s.PatternID + "-" + s.EdgeID
}

// Display formats the signature for the failure footer.
// Example: a1b2c3d4-e4f9c2a1  (prometheus→postgres · no_stream_columns)
func (s Signature) Display() string {
	src := typeToken(s.Meta.SourceType)
	tgt := typeToken(s.Meta.TargetType)
	label := s.ShortLabel
	if label == "" {
		label = "unknown"
	}
	return fmt.Sprintf("%s  (%s→%s · %s)", s.IDDashed(), src, tgt, label)
}

// ParseSignatureID normalizes user input into compact 8 or 16 hex lowercase.
// Accepts "aabbccdd", "aabbccdd-eeff0011", "aabbccddeeff0011", or a Display() line.
// Returns compact hex and whether it is pattern-only (len 8) vs full composite (len 16).
func ParseSignatureID(raw string) (compact string, patternOnly bool, err error) {
	s := strings.ToLower(strings.TrimSpace(raw))
	if i := strings.IndexAny(s, " \t("); i > 0 {
		s = s[:i]
	}
	s = strings.ReplaceAll(s, "-", "")
	if len(s) != PartIDLen && len(s) != CompositeIDLen {
		return "", false, fmt.Errorf("invalid error signature %q (expected 8 or 16 hex chars)", raw)
	}
	for _, r := range s {
		if r < '0' || (r > '9' && r < 'a') || r > 'f' {
			return "", false, fmt.Errorf("invalid error signature %q (expected hex)", raw)
		}
	}
	return s, len(s) == PartIDLen, nil
}

// InferredTaskType returns the job-type slug (db-db, file-db, api-file, …)
// derived from connector kinds. Empty when either side is unknown.
// Not part of the hash material — source+target types already encode this.
func (m SignMeta) InferredTaskType() string {
	m = normalizeMeta(m)
	sk, tk := kindAbbrev(m.SourceType.Kind()), kindAbbrev(m.TargetType.Kind())
	if sk == "" || tk == "" {
		return ""
	}
	return sk + "-" + tk
}

func kindAbbrev(k dbio.Kind) string {
	switch k {
	case dbio.KindDatabase:
		return "db"
	case dbio.KindFile:
		return "file"
	case dbio.KindAPI:
		return "api"
	default:
		return ""
	}
}

func normalizeMeta(m SignMeta) SignMeta {
	return SignMeta{
		SourceType: dbio.Type(strings.ToLower(strings.TrimSpace(string(m.SourceType)))),
		TargetType: dbio.Type(strings.ToLower(strings.TrimSpace(string(m.TargetType)))),
	}
}

func typeToken(t dbio.Type) string {
	s := strings.TrimSpace(string(t))
	if s == "" {
		return "-"
	}
	return s
}

// PatternMaterial builds the pattern-layer hash input (skeleton only).
//
//	v1p|<skeleton>
func PatternMaterial(skeleton string) string {
	if skeleton == "" {
		skeleton = "unknown_error"
	}
	return PatternMaterialPrefix + "|" + skeleton
}

// EdgeMaterial builds the edge-layer hash input (types + skeleton).
//
//	v1e|<source>|<target>|<skeleton>
//
// Task type is omitted: it is redundant given source and target connector types.
func EdgeMaterial(skeleton string, meta SignMeta) string {
	meta = normalizeMeta(meta)
	if skeleton == "" {
		skeleton = "unknown_error"
	}
	return strings.Join([]string{
		EdgeMaterialPrefix,
		typeToken(meta.SourceType),
		typeToken(meta.TargetType),
		skeleton,
	}, "|")
}

// Material is an alias for EdgeMaterial (edge-layer input).
func Material(skeleton string, meta SignMeta) string {
	return EdgeMaterial(skeleton, meta)
}

// HashPart returns the first PartIDLen hex chars of SHA-256(material).
func HashPart(material string) string {
	sum := sha256.Sum256([]byte(material))
	return hex.EncodeToString(sum[:])[:PartIDLen]
}

// HashMaterial is an alias for HashPart.
func HashMaterial(material string) string {
	return HashPart(material)
}

// ShortLabel is a human-only slug from the skeleton (not part of the hash).
func ShortLabel(skeleton string) string {
	if skeleton == "" || skeleton == "unknown_error" {
		return "unknown_error"
	}
	lines := strings.Split(skeleton, "\n")
	// Prefer a vendor code when present (stable, short).
	for i := len(lines) - 1; i >= 0; i-- {
		if strings.HasPrefix(lines[i], "code:") {
			return slugLabel(strings.TrimPrefix(lines[i], "code:"))
		}
	}
	// Else last non-empty message line (usually the leaf driver message).
	pick := lines[0]
	for i := len(lines) - 1; i >= 0; i-- {
		l := lines[i]
		if l == "" {
			continue
		}
		pick = l
		break
	}
	return slugLabel(pick)
}

func slugLabel(pick string) string {
	// Slugify: non-alnum → _, collapse
	var b strings.Builder
	prevUnderscore := false
	for _, r := range pick {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
			prevUnderscore = false
			continue
		}
		if !prevUnderscore {
			b.WriteByte('_')
			prevUnderscore = true
		}
	}
	s := strings.Trim(b.String(), "_")
	if len(s) > 40 {
		s = s[:40]
		s = strings.TrimRight(s, "_")
	}
	if s == "" {
		return "unknown_error"
	}
	return s
}

// --- normalization -----------------------------------------------------------

var (
	// Stack frames: --- task_run.go:140 func2 ---
	reStackFrame = regexp.MustCompile(`(?i)^---\s+\S+\.go:\d+\s+`)

	// Vendor / driver codes (uppercase-ish tokens in brackets or parentheses)
	reBracketCode = regexp.MustCompile(`\[([A-Z][A-Z0-9_.]+)\]`)
	reParenCode   = regexp.MustCompile(`\(([A-Z][A-Z0-9_]{2,})\)`)
	reSQLState    = regexp.MustCompile(`(?i)\bSQLSTATE[:\s]+([0-9A-Z]{5})\b`)
	reCHCode      = regexp.MustCompile(`\bCode:\s*(\d+)\b`)

	// Placeholders — order matters (more specific first).
	// Quoted URL / path before bare forms so later "<ident>" does not swallow them.
	// URLs on slingdata.io (or subdomains) are kept as-is (see replaceURL).
	reQuotedURL  = regexp.MustCompile(`(?i)"(?:https?|s3|gs|file|azure|abfs|abfss)://[^"]*"`)
	reQuotedPath = regexp.MustCompile(`"(?:/|~/|~\\)[^"]*"`)
	reURL        = regexp.MustCompile(`(?i)\b(?:https?|s3|gs|file|azure|abfs|abfss)://[^\s"'<>]+`)
	// Absolute / home paths (unix + windows drive). Match path token only.
	reUnixPath = regexp.MustCompile(`(?:^|[\s"'=(])(/[^\s"'<>]+)`)
	reWinPath  = regexp.MustCompile(`(?i)(?:^|[\s"'=(])([a-z]:\\[^\s"'<>]+)`)
	reHomePath = regexp.MustCompile(`(?:^|[\s"'=(])(~/[^\s"'<>]*)`)
	reUUID     = regexp.MustCompile(`(?i)\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b`)
	// ksuid / sling exec ids / long hex (exclude already-replaced tokens)
	reLongID = regexp.MustCompile(`\b(?:exec_[A-Za-z0-9]+|[0-9A-Za-z]{24,}|[0-9a-fA-F]{16,})\b`)
	reISOTs  = regexp.MustCompile(`\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?\b`)
	// IPv4 (+ optional port)
	reIPv4 = regexp.MustCompile(`\b\d{1,3}(?:\.\d{1,3}){3}(?::\d{1,5})?\b`)
	// Quoted / backtick identifiers (no spaces — keeps prose messages intact).
	// Matches schema.table, columns, etc. after URL/path quoted forms.
	reIdentDQ = regexp.MustCompile(`"[^"\s]{1,256}"`)
	// Backtick pattern assembled so raw-string delimiters do not collide.
	reIdentBT = regexp.MustCompile("`" + `[^` + "`" + `\s]{1,256}` + "`")
	// Temp table names sling generates
	reTempTable = regexp.MustCompile(`\btemp[A-Za-z0-9]{3,}\b`)
	// Version banners from drivers
	reVersionBanner = regexp.MustCompile(`\(version\s+[^)]+\)`)
	// Long digit runs (≥3) — last, so short codes survive earlier patterns
	reDigits = regexp.MustCompile(`\b\d{3,}\b`)

	reMultiSpace = regexp.MustCompile(`[ \t]+`)
)

// Skeleton normalizes a full error chain into a stable multi-line skeleton.
func Skeleton(errText string) string {
	rawLines := strings.Split(errText, "\n")
	kept := make([]string, 0, len(rawLines))
	codeSet := map[string]struct{}{}
	var codes []string

	for _, raw := range rawLines {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		if reStackFrame.MatchString(line) {
			continue
		}
		// Stream/section banners like "----- name -----"
		if isSectionBanner(line) {
			continue
		}
		if strings.HasPrefix(line, "~ ") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "~ "))
		} else if strings.HasPrefix(line, "~") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "~"))
		}
		if line == "" {
			continue
		}

		for _, c := range extractCodes(line) {
			if _, ok := codeSet[c]; !ok {
				codeSet[c] = struct{}{}
				codes = append(codes, c)
			}
		}

		line = substitutePlaceholders(line)
		line = strings.ToLower(line)
		line = reMultiSpace.ReplaceAllString(line, " ")
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		kept = append(kept, line)
	}

	if len(kept) == 0 && len(codes) == 0 {
		return "unknown_error"
	}

	// Append stable vendor codes last so the leaf message stays first for labels.
	for _, c := range codes {
		kept = append(kept, "code:"+strings.ToLower(c))
	}
	return strings.Join(kept, "\n")
}

func isSectionBanner(line string) bool {
	// e.g. --------------------------- lxp_app_oomkilled_count ---------------------------
	if len(line) < 10 {
		return false
	}
	trimmed := strings.Trim(line, "- \t")
	if trimmed == "" {
		return false
	}
	// mostly dashes on the outside
	return strings.HasPrefix(strings.TrimSpace(line), "---") &&
		strings.HasSuffix(strings.TrimSpace(line), "---") &&
		!reStackFrame.MatchString(line)
}

func extractCodes(line string) []string {
	var out []string
	for _, m := range reBracketCode.FindAllStringSubmatch(line, -1) {
		out = append(out, m[1])
	}
	for _, m := range reParenCode.FindAllStringSubmatch(line, -1) {
		// Skip common non-code paren groups
		c := m[1]
		if c == "official" || c == "build" {
			continue
		}
		out = append(out, c)
	}
	for _, m := range reSQLState.FindAllStringSubmatch(line, -1) {
		out = append(out, m[1])
	}
	for _, m := range reCHCode.FindAllStringSubmatch(line, -1) {
		out = append(out, "CH_"+m[1])
	}
	return out
}

// replaceURL masks a URL match, unless it points at slingdata.io (or a subdomain).
func replaceURL(match string) string {
	u := strings.Trim(match, `"`)
	if i := strings.Index(u, "://"); i >= 0 {
		host := u[i+3:]
		if j := strings.IndexAny(host, "/?#"); j >= 0 {
			host = host[:j]
		}
		if j := strings.LastIndex(host, ":"); j >= 0 {
			host = host[:j]
		}
		host = strings.ToLower(host)
		if host == "slingdata.io" || strings.HasSuffix(host, ".slingdata.io") {
			return match
		}
	}
	return "<url>"
}

func substitutePlaceholders(line string) string {
	// Order: more specific first.
	line = reQuotedURL.ReplaceAllStringFunc(line, replaceURL)
	line = reQuotedPath.ReplaceAllString(line, "<path>")
	line = reURL.ReplaceAllStringFunc(line, replaceURL)
	line = reISOTs.ReplaceAllString(line, "<ts>")
	line = reUUID.ReplaceAllString(line, "<id>")
	line = reIPv4.ReplaceAllString(line, "<ip>")
	line = reVersionBanner.ReplaceAllString(line, "")
	line = reTempTable.ReplaceAllString(line, "<temp>")
	line = reLongID.ReplaceAllString(line, "<id>")
	line = reIdentDQ.ReplaceAllString(line, "<ident>")
	line = reIdentBT.ReplaceAllString(line, "<ident>")
	// Bare paths: keep leading delimiter, replace path token.
	line = replacePathKeepLead(reUnixPath, line)
	line = replacePathKeepLead(reWinPath, line)
	line = replacePathKeepLead(reHomePath, line)
	line = reDigits.ReplaceAllString(line, "<n>")
	return line
}

// replacePathKeepLead replaces path matches of the form (lead)(path) with lead+"<path>".
func replacePathKeepLead(re *regexp.Regexp, line string) string {
	return re.ReplaceAllStringFunc(line, func(m string) string {
		sub := re.FindStringSubmatch(m)
		if len(sub) < 2 {
			return m
		}
		path := sub[1]
		lead := strings.TrimSuffix(m, path)
		return lead + "<path>"
	})
}

// --- assist error lookup -----------------------------------------------------

// ErrorLookupResult is the response shape for `sling assist error <sig>`.
// Worker-backed known-issue lookup lands later; v1 returns local status only.
type ErrorLookupResult struct {
	Signature   string `json:"signature"`              // compact 8 or 16 hex
	PatternID   string `json:"pattern_id,omitempty"`   // first 8 of composite
	EdgeID      string `json:"edge_id,omitempty"`      // last 8 when full composite
	PatternOnly bool   `json:"pattern_only,omitempty"` // true when caller passed 8 hex
	Status      string `json:"status"`                 // known_config | known_bug | pending | unknown
	Title       string `json:"title,omitempty"`
	Guidance    string `json:"guidance,omitempty"`
	DocsURL     string `json:"docs_url,omitempty"`
	FixedIn     string `json:"fixed_in,omitempty"`
	IssueURL    string `json:"issue_url,omitempty"`
}

// LookupError validates a signature id and returns guidance when known.
// Accepts composite (16 hex), pattern-only (8 hex), dashed form, or Display() line.
// Network lookup is not wired yet; well-formed ids return status "unknown".
func LookupError(signature string) (ErrorLookupResult, error) {
	compact, patternOnly, err := ParseSignatureID(signature)
	if err != nil {
		return ErrorLookupResult{}, err
	}
	out := ErrorLookupResult{
		Signature:   compact,
		PatternOnly: patternOnly,
		Status:      "unknown",
		Title:       "No published guidance yet",
		Guidance:    "This signature is not in the known-issue registry yet. Run `sling assist` to debug the latest failed run locally, or check docs.slingdata.io.",
		DocsURL:     "https://docs.slingdata.io/",
	}
	if patternOnly {
		out.PatternID = compact
	} else {
		out.PatternID = compact[:PartIDLen]
		out.EdgeID = compact[PartIDLen:]
	}
	return out, nil
}

// Sensitivity classifies what may leave the machine in a log/submit bundle.
type Sensitivity int

const (
	// SensitivityPublic is safe to ship as-is (no credentials by design).
	SensitivityPublic Sensitivity = iota
	// SensitivityInternal may contain operational detail; redact values before ship.
	SensitivityInternal
	// SensitivitySecret must never leave the machine (credentials, tokens, backups of same).
	SensitivitySecret
)

func (s Sensitivity) String() string {
	switch s {
	case SensitivityPublic:
		return "public"
	case SensitivityInternal:
		return "internal"
	case SensitivitySecret:
		return "secret"
	default:
		return "unknown"
	}
}

// SensitiveClass describes one path/category the assist package may touch.
type SensitiveClass struct {
	// ID is a stable key for manifests (e.g. "env.yaml", "claude.json").
	ID string `json:"id"`
	// Glob is matched against absolute or home-relative paths (slash-normalized).
	// Supports * and ** suffix style via pathMatch.
	Glob string `json:"glob"`
	// Class is the sensitivity tier.
	Class Sensitivity `json:"class"`
	// Reason is a short human explanation (never contains secret values).
	Reason string `json:"reason"`
}

// SensitivityManifest returns the static inventory of sensitive surface area.
// Used by submit/bundle builders to decide include / redact / exclude.
func SensitivityManifest() []SensitiveClass {
	return []SensitiveClass{
		{
			ID: "env.yaml", Glob: "**/env.yaml", Class: SensitivitySecret,
			Reason: "may contain connection credentials and env secrets",
		},
		{
			ID: "claude.json", Glob: "**/.claude.json", Class: SensitivitySecret,
			Reason: "Claude Code OAuth session and user MCP config",
		},
		{
			ID: "claude-mcp-project", Glob: "**/.mcp.json", Class: SensitivityInternal,
			Reason: "project MCP server definitions; may reference env vars",
		},
		{
			ID: "codex-config", Glob: "**/.codex/config.toml", Class: SensitivityInternal,
			Reason: "may include MCP env blocks",
		},
		{
			ID: "gemini-settings", Glob: "**/.gemini/settings.json", Class: SensitivityInternal,
			Reason: "MCP and model settings",
		},
		{
			ID: "cursor-mcp", Glob: "**/.cursor/mcp.json", Class: SensitivityInternal,
			Reason: "MCP server definitions",
		},
		{
			ID: "vscode-mcp", Glob: "**/mcp.json", Class: SensitivityInternal,
			Reason: "VS Code MCP servers (user or .vscode)",
		},
		{
			ID: "config-backup", Glob: "**/*.backup", Class: SensitivitySecret,
			Reason: "backups of credential-bearing config files",
		},
		{
			ID: "run-error", Glob: "**/assist/errors/**/error.txt", Class: SensitivityInternal,
			Reason: "error chains may embed query fragments or object names",
		},
		{
			ID: "run-stderr", Glob: "**/assist/errors/**/stderr.log", Class: SensitivityInternal,
			Reason: "debug logs may include connection props if not redacted at write",
		},
		{
			ID: "run-meta", Glob: "**/assist/errors/**/meta.json", Class: SensitivityPublic,
			Reason: "exec metadata; argv must be redacted at write time",
		},
		{
			ID: "run-config-snapshot", Glob: "**/assist/errors/**/config.snapshot.yaml", Class: SensitivityInternal,
			Reason: "resolved config; secrets must be masked at write time",
		},
		{
			ID: "doctor.json", Glob: "**/doctor.json", Class: SensitivityPublic,
			Reason: "install health only; no connection secrets",
		},
		{
			ID: "assist-history-prompt", Glob: "**/assist/history/*/prompt.md", Class: SensitivityInternal,
			Reason: "user intention and log tails; may include business context",
		},
		{
			ID: "canonical-skills", Glob: "**/.agents/skills/**", Class: SensitivityPublic,
			Reason: "embedded public skill docs",
		},
	}
}

// ClassifyPath returns the highest-sensitivity class matching path.
// Unknown paths default to SensitivityInternal (safe default: redact before ship).
// When multiple globs match, Secret > Internal > Public.
func ClassifyPath(path string) Sensitivity {
	n := filepath.ToSlash(path)
	matched := false
	best := SensitivityPublic
	for _, c := range SensitivityManifest() {
		if !pathMatch(c.Glob, n) {
			continue
		}
		matched = true
		if c.Class == SensitivitySecret {
			return SensitivitySecret
		}
		if c.Class == SensitivityInternal {
			best = SensitivityInternal
		}
	}
	if !matched {
		return SensitivityInternal
	}
	return best
}

// pathMatch supports a small glob dialect used by SensitivityManifest:
//   - "**/" prefix = match anywhere
//   - "*" = one path segment
//   - "**" = one or more segments (including zero when trailing)
//   - "*.ext" basename wildcards
func pathMatch(glob, path string) bool {
	glob = filepath.ToSlash(glob)
	path = filepath.ToSlash(path)
	if glob == path {
		return true
	}
	gParts := strings.Split(glob, "/")
	pParts := strings.Split(path, "/")
	return matchParts(gParts, pParts)
}

func matchParts(gParts, pParts []string) bool {
	// Recursive glob matcher for segment lists.
	var rec func(gi, pi int) bool
	rec = func(gi, pi int) bool {
		for gi < len(gParts) {
			g := gParts[gi]
			if g == "**" {
				// "**" matches zero or more segments.
				if gi == len(gParts)-1 {
					return true // trailing **
				}
				// Try consuming 0..N path segments.
				for k := pi; k <= len(pParts); k++ {
					if rec(gi+1, k) {
						return true
					}
				}
				return false
			}
			if pi >= len(pParts) {
				return false
			}
			if g == "*" || matchSeg(g, pParts[pi]) {
				gi++
				pi++
				continue
			}
			return false
		}
		return pi == len(pParts)
	}
	return rec(0, 0)
}

func matchSeg(pat, seg string) bool {
	if pat == "*" || pat == seg {
		return true
	}
	// basename wildcard: *.backup
	if strings.HasPrefix(pat, "*.") {
		return strings.HasSuffix(seg, pat[1:]) // ".backup"
	}
	if strings.Contains(pat, "*") {
		// simple prefix*suffix
		i := strings.Index(pat, "*")
		return strings.HasPrefix(seg, pat[:i]) && strings.HasSuffix(seg, pat[i+1:])
	}
	return false
}

// FailureSnapshot is the minimal set of fields written when a command fails
// (run, conns test, conns discover) so `sling assist` can probe the error.
type FailureSnapshot struct {
	ExecID     string
	ErrMsg     string
	ConfigPath string // replication / pipeline config path when known
	ConnName   string // connection name for `sling conns test|discover`
	Rows       string
	Duration   string
	// RunLog is the captured log tail (env.RecentLogs). Written to stderr.log.
	RunLog string
	// ConfigBody is the replication/pipeline config file content. Written to
	// config.snapshot.yaml when set. Not used for conns test/discover.
	ConfigBody string
	// SignMeta optional connector types for error_signature.
	SignMeta SignMeta
	// Extra is merged into meta.json as-is (connector types, etc.).
	Extra map[string]any
}

const (
	reservedExecutionsDir = "executions"
	reservedSignaturesDir = "signatures"
)

func isReservedErrorName(name string) bool {
	return name == reservedExecutionsDir || name == reservedSignaturesDir
}

// WriteFailureSnapshot writes ~/.sling/assist/errors/executions/<exec_id>/{meta.json,
// error.txt, stderr.log}. Best-effort: no-op when execID is empty; never fails
// the caller run (errors are logged via g.Debug only).
func WriteFailureSnapshot(s FailureSnapshot) {
	if strings.TrimSpace(s.ExecID) == "" {
		return
	}

	// load config body
	if ext := strings.ToLower(filepath.Ext(s.ConfigPath)); g.In(ext, ".yaml", ".yml", ".json") && s.ConfigBody == "" {
		if b, err := os.ReadFile(s.ConfigPath); err == nil && int64(len(b)) <= 64*1024 {
			s.ConfigBody = string(b)
		}
	}

	dir := filepath.Join(ExecutionsDir(), s.ExecID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		g.Debug("assist: could not create error dir %s: %s", dir, err.Error())
		return
	}

	errMsg := s.ErrMsg
	if errMsg == "" {
		errMsg = "(no error message captured)"
	}
	if err := os.WriteFile(filepath.Join(dir, "error.txt"), []byte(errMsg), 0o644); err != nil {
		g.Debug("assist: could not write error.txt: %s", err.Error())
	}
	// stderr.log holds the captured run log. Fall back to the error text when
	// nothing was buffered, so the file is never empty.
	runLog := s.RunLog
	if strings.TrimSpace(runLog) == "" {
		runLog = errMsg
	}
	if err := os.WriteFile(filepath.Join(dir, "stderr.log"), []byte(runLog), 0o644); err != nil {
		g.Debug("assist: could not write stderr.log: %s", err.Error())
	}
	if body := strings.TrimSpace(s.ConfigBody); body != "" {
		if err := os.WriteFile(filepath.Join(dir, "config.snapshot.yaml"), []byte(body), 0o644); err != nil {
			g.Debug("assist: could not write config.snapshot.yaml: %s", err.Error())
		}
	}

	sig := SignError(errMsg, s.SignMeta)

	meta := map[string]any{
		"exec_id":           s.ExecID,
		"exit_code":         1,
		"when":              time.Now().UTC().Format(time.RFC3339),
		"error_signature":   sig.ID, // 16 hex: pattern||edge
		"error_pattern_id":  sig.PatternID,
		"error_edge_id":     sig.EdgeID,
		"error_algorithm":   sig.Algorithm,
		"error_short_label": sig.ShortLabel,
	}
	if s.SignMeta.SourceType != "" {
		meta["source_type"] = s.SignMeta.SourceType.String()
	}
	if s.SignMeta.TargetType != "" {
		meta["target_type"] = s.SignMeta.TargetType.String()
	}
	if tt := s.SignMeta.InferredTaskType(); tt != "" {
		meta["task_type"] = tt // derived; not part of signature hash
	}
	if s.ConfigPath != "" {
		meta["config_path"] = s.ConfigPath
	}
	if s.ConnName != "" {
		meta["conn_name"] = s.ConnName
	}
	if s.Rows != "" {
		meta["rows"] = s.Rows
	}
	if s.Duration != "" {
		meta["duration"] = s.Duration
	}
	for k, v := range s.Extra {
		if _, exists := meta[k]; !exists {
			meta[k] = v
		}
	}
	body, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		g.Debug("assist: could not marshal meta.json: %s", err.Error())
		return
	}
	if err := os.WriteFile(filepath.Join(dir, "meta.json"), body, 0o644); err != nil {
		g.Debug("assist: could not write meta.json: %s", err.Error())
	}

	if err := AutoTrimExecs(); err != nil {
		g.Debug("assist: auto-trim execs: %s", err.Error())
	}
}

// FailureFooterOpts controls the post-failure hint line.
// A command only — never an interactive prompt (TTY or not).
type FailureFooterOpts struct {
	ExecID   string
	ErrMsg   string
	SignMeta SignMeta
}

// PrintFailureFooter prints one indented hint line after a failure.
// The error signature is not printed: it is agent context, written to
// meta.json by WriteFailureSnapshot and read back by Probe.
// Suppressed when HintInErrors is false or SLING_ASSIST_HINT is falsey.
func PrintFailureFooter(opts FailureFooterOpts) {
	if envDisabled("SLING_ASSIST_HINT") {
		return
	}

	prof, exists, err := LoadProfile()
	if err != nil {
		return
	}
	hintOn := true
	if exists {
		hintOn = prof.HintInErrors
	}
	if !hintOn {
		return
	}

	line := "  sling assist setup"
	if execID := strings.TrimSpace(opts.ExecID); execID != "" {
		line = "  sling assist --id " + ShortExecID(execID)
	} else if exists || len(DetectedClients()) > 0 {
		return
	}

	if isTTY(os.Stderr) && !env.NoColor {
		label := terminalLink(AssistDocsURL, "investigate with AI")
		line = "  " + label + " -> " + env.CyanString(strings.TrimSpace(line))
	}
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, line)
	fmt.Fprintln(os.Stderr, "")
}

// AssistDocsURL is linked from the failure hint.
const AssistDocsURL = "https://docs.slingdata.io/sling-cli/assist"

// terminalLink wraps text in an OSC 8 hyperlink. Terminals without OSC 8
// support drop the escape codes and show only the text.
func terminalLink(url, text string) string {
	return "\x1b]8;;" + url + "\x1b\\" + text + "\x1b]8;;\x1b\\"
}

// ShortExecIDLen is the exec-id prefix length shown in the failure hint.
// ResolveLocalExec accepts any unique prefix.
const ShortExecIDLen = 8

// ShortExecID trims an exec id to the prefix shown to users.
func ShortExecID(id string) string {
	id = strings.TrimSpace(id)
	if len(id) > ShortExecIDLen {
		return id[:ShortExecIDLen]
	}
	return id
}

// MaybePrintErrorHint is kept for callers/tests that only have an exec id.
// Prefer PrintFailureFooter when error text is available.
func MaybePrintErrorHint(execID string) {
	PrintFailureFooter(FailureFooterOpts{ExecID: execID})
}

// envDisabled is true when the named env var is set to a falsey value.
func envDisabled(key string) bool {
	v := os.Getenv(key)
	if v == "" {
		return false
	}
	return !cast.ToBool(v)
}

// LocalExec is one failed-run snapshot under ~/.sling/assist/errors/.
// New snapshots live in errors/executions/<id>/; legacy dirs stay readable.
type LocalExec struct {
	ID         string
	When       time.Time
	Status     string // "ok" | "err" | "?"
	ConfigPath string // replication / pipeline config path
	ConnName   string // connection name for conns test/discover
	Rows       string
	Duration   string
	LogDir     string // absolute path to the exec's snapshot dir
}

func (e LocalExec) displayObject() string {
	if e.ConfigPath != "" {
		return e.ConfigPath
	}
	return e.ConnName
}

// LogsRoot returns ~/.sling/logs (SLING_LOG_DIR day files). Not used for failure snapshots.
func LogsRoot() string {
	return filepath.Join(slingHome(), "logs")
}

// ListLocalExecs scans errors/executions/<id>/ then legacy errors/<id>/
// and returns the 20 most-recent execs by mtime. Unreadable dirs are skipped.
func ListLocalExecs() ([]LocalExec, error) {
	ids, err := listLocalExecIDs()
	if err != nil {
		return nil, err
	}
	out := []LocalExec{}
	for _, id := range ids {
		dir := findLocalExecDir(id)
		if dir == "" {
			continue
		}
		le := LocalExec{ID: id, LogDir: dir, Status: "?"}
		if info, err := os.Stat(dir); err == nil {
			le.When = info.ModTime()
		}
		loadLocalExecMeta(&le)
		out = append(out, le)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].When.After(out[j].When)
	})
	if len(out) > 20 {
		out = out[:20]
	}
	return out, nil
}

// AutoTrimExecs deletes the oldest failure snapshots until at most
// ExecsMaxEntries remain. Covers both errors/executions/<id>/ and the legacy
// errors/<id>/ layout. Best-effort: a snapshot that cannot be removed is
// reported, and the rest still get trimmed.
func AutoTrimExecs() error {
	ids, err := listLocalExecIDs()
	if err != nil {
		return err
	}
	if len(ids) <= ExecsMaxEntries {
		return nil
	}

	type entry struct {
		dir  string
		when time.Time
	}
	entries := make([]entry, 0, len(ids))
	for _, id := range ids {
		dir := findLocalExecDir(id)
		if dir == "" {
			continue
		}
		e := entry{dir: dir}
		if info, statErr := os.Stat(dir); statErr == nil {
			e.when = info.ModTime()
		}
		entries = append(entries, e)
	}
	if len(entries) <= ExecsMaxEntries {
		return nil
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].when.After(entries[j].when)
	})

	var first error
	for _, e := range entries[ExecsMaxEntries:] {
		if rmErr := os.RemoveAll(e.dir); rmErr != nil && first == nil {
			first = g.Error(rmErr, "remove exec snapshot %s", e.dir)
		}
	}
	return first
}

func listLocalExecIDs() ([]string, error) {
	seen := map[string]struct{}{}
	var ids []string
	addFrom := func(root string, skipReserved bool) error {
		if root == "" || !g.PathExists(root) {
			return nil
		}
		entries, err := os.ReadDir(root)
		if err != nil {
			return g.Error(err, "read %s", root)
		}
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			name := e.Name()
			if skipReserved && isReservedErrorName(name) {
				continue
			}
			if _, ok := seen[name]; ok {
				continue
			}
			seen[name] = struct{}{}
			ids = append(ids, name)
		}
		return nil
	}
	if err := addFrom(ExecutionsDir(), false); err != nil {
		return nil, err
	}
	if err := addFrom(ErrorsDir(), true); err != nil {
		return nil, err
	}
	return ids, nil
}

func firstMetaString(doc map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, _ := doc[k].(string); strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func loadLocalExecMeta(e *LocalExec) {
	metaPath := filepath.Join(e.LogDir, "meta.json")
	doc, err := jsonReadOrEmpty(metaPath)
	if err != nil || len(doc) == 0 {
		return
	}
	if v, ok := doc["exit_code"]; ok {
		if fmt.Sprintf("%v", v) == "0" {
			e.Status = "ok"
		} else {
			e.Status = "err"
		}
	}
	if v := firstMetaString(doc, "config_path", "object"); v != "" {
		e.ConfigPath = v
	}
	if v, _ := doc["conn_name"].(string); v != "" {
		e.ConnName = v
	}
	if v, _ := doc["rows"].(string); v != "" {
		e.Rows = v
	}
	if v, _ := doc["duration"].(string); v != "" {
		e.Duration = v
	}
}

// LookupLocalExec resolves an exec id to its snapshot. Accepts the full id or
// a unique prefix. Returns false when unknown or when a prefix is ambiguous.
func LookupLocalExec(id string) (LocalExec, bool) {
	le, err := ResolveLocalExec(id)
	return le, err == nil
}

// ResolveLocalExec is LookupLocalExec with a reason: unknown vs ambiguous.
func ResolveLocalExec(id string) (LocalExec, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return LocalExec{}, g.Error("empty exec id")
	}
	if dir := findLocalExecDir(id); dir != "" {
		le := LocalExec{ID: id, LogDir: dir, Status: "?"}
		if info, err := os.Stat(dir); err == nil {
			le.When = info.ModTime()
		}
		loadLocalExecMeta(&le)
		return le, nil
	}
	// Prefix match against every snapshot, not just the 20 ListLocalExecs keeps.
	ids, err := listLocalExecIDs()
	if err != nil {
		return LocalExec{}, g.Error("unknown exec id %q", id)
	}
	hits := []string{}
	for _, name := range ids {
		if strings.HasPrefix(name, id) {
			hits = append(hits, name)
		}
	}
	switch len(hits) {
	case 0:
		return LocalExec{}, g.Error("unknown exec id %q", id)
	case 1:
		return ResolveLocalExec(hits[0])
	default:
		sort.Strings(hits)
		return LocalExec{}, g.Error("exec id %q is ambiguous (%d matches: %s)",
			id, len(hits), strings.Join(hits[:2], ", ")+", …")
	}
}

// findLocalExecDir returns the snapshot dir for id. New layout first, then legacy.
func findLocalExecDir(id string) string {
	if id == "" || isReservedErrorName(id) {
		return ""
	}
	if strings.ContainsAny(id, `/\`) || strings.Contains(id, "..") {
		return ""
	}
	if candidate := filepath.Join(ExecutionsDir(), id); g.PathExists(candidate) {
		return candidate
	}
	if candidate := filepath.Join(ErrorsDir(), id); g.PathExists(candidate) {
		return candidate
	}
	return ""
}

const maxErrorTailBytes = 16 * 1024

// sanitizeLogForPrompt caps length, scrubs local connection secrets, and
// neutralizes triple-backtick fences so hostile log content cannot escape
// the markdown code blocks in prompts.yaml.
func sanitizeLogForPrompt(s string, maxBytes int) string {
	s = scrubLocalConnSecrets(s)
	if maxBytes > 0 && len(s) > maxBytes {
		// Keep the tail (errors are usually at the end).
		s = s[len(s)-maxBytes:]
		if i := strings.IndexByte(s, '\n'); i >= 0 && i < 200 {
			s = s[i+1:]
		}
		s = "[...truncated...]\n" + s
	}
	// Break ``` fences so log content cannot close the surrounding fence.
	s = strings.ReplaceAll(s, "```", "'''")
	return s
}

func scrubLocalConnSecrets(s string) string {
	return env.ScrubLine(s)
}
