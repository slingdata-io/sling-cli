package assist

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/flarco/g"
	"github.com/tidwall/gjson"
	"github.com/tidwall/jsonc"
	"github.com/tidwall/sjson"
)

// jsonReadOrEmpty parses JSON/JSONC into a map. Missing/empty → empty map.
func jsonReadOrEmpty(path string) (map[string]any, error) {
	out := map[string]any{}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return out, nil
		}
		return nil, g.Error(err, "read %s", path)
	}
	if len(data) == 0 {
		return out, nil
	}
	stripped := jsonc.ToJSON(data)
	if err := json.Unmarshal(stripped, &out); err != nil {
		return nil, g.Error(err, "parse %s", path)
	}
	return out, nil
}

// jsonWritePretty writes sling-owned JSON (no user comments to preserve).
// For user configs use setJSONPath/deleteJSONPath.
func jsonWritePretty(path string, m map[string]any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return g.Error(err, "mkdir %s", filepath.Dir(path))
	}
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return g.Error(err, "marshal %s", path)
	}
	data = append(data, '\n')
	return writeBytesPreserveMode(path, data, 0o644)
}

// jsonReadRaw reads JSON/JSONC for surgical sjson edits.
// Missing/empty → "{}". Leading non-JSON (e.g. VS Code banner) is split off
// so sjson does not compact the whole file; caller re-prepends via jsonWriteRaw.
func jsonReadRaw(path string) (prefix, body []byte, err error) {
	data, rerr := os.ReadFile(path)
	if rerr != nil {
		if os.IsNotExist(rerr) {
			return nil, []byte("{}"), nil
		}
		return nil, nil, g.Error(rerr, "read %s", path)
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, []byte("{}"), nil
	}
	prefix, body = splitLeadingNonJSON(data)
	return prefix, body, nil
}

// splitLeadingNonJSON splits at the first `{` or `[`.
func splitLeadingNonJSON(data []byte) (prefix, body []byte) {
	for i, c := range data {
		if c == '{' || c == '[' {
			if i == 0 {
				return nil, data
			}
			return data[:i], data[i:]
		}
	}
	return data, nil
}

// jsonWriteRaw writes prefix+body, preserving mode (default 0600 for new files).
func jsonWriteRaw(path string, prefix, body []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return g.Error(err, "mkdir %s", filepath.Dir(path))
	}
	out := body
	if len(prefix) > 0 {
		out = append(append([]byte{}, prefix...), body...)
	}
	if len(out) == 0 || out[len(out)-1] != '\n' {
		out = append(out, '\n')
	}
	return writeBytesPreserveMode(path, out, 0o600)
}

var sjsonOpts = &sjson.Options{Optimistic: true}

const backupSuffix = ".backup"

func fileMode(path string, def os.FileMode) os.FileMode {
	info, err := os.Stat(path)
	if err != nil {
		return def
	}
	return info.Mode().Perm()
}

// writeBytesPreserveMode writes data, keeping existing mode when overwriting.
func writeBytesPreserveMode(path string, data []byte, defMode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return g.Error(err, "mkdir %s", filepath.Dir(path))
	}
	mode := fileMode(path, defMode)
	return os.WriteFile(path, data, mode)
}

// backupBeforeEdit copies path → path.backup (no-op if missing).
func backupBeforeEdit(path string) error {
	src, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return g.Error(err, "read %s for backup", path)
	}
	if len(src) == 0 {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return g.Error(err, "mkdir %s", filepath.Dir(path))
	}
	mode := fileMode(path, 0o600)
	if err := os.WriteFile(path+backupSuffix, src, mode); err != nil {
		return g.Error(err, "write %s", path+backupSuffix)
	}
	return nil
}

func restoreBackup(path string) error {
	src, err := os.ReadFile(path + backupSuffix)
	if err != nil {
		return g.Error(err, "read %s", path+backupSuffix)
	}
	mode := fileMode(path+backupSuffix, fileMode(path, 0o600))
	return os.WriteFile(path, src, mode)
}

func countTopLevelKeys(data []byte) int {
	clean := jsonc.ToJSON(data)
	res := gjson.ParseBytes(clean)
	if !res.IsObject() {
		return 0
	}
	n := 0
	res.ForEach(func(_, _ gjson.Result) bool {
		n++
		return true
	})
	return n
}

func countLines(data []byte) int {
	if len(data) == 0 {
		return 0
	}
	n := 1
	for _, c := range data {
		if c == '\n' {
			n++
		}
	}
	return n
}

// validateEditNotDestructive refuses edits that drop top-level keys or
// collapse multi-line docs (sjson banner bug). allowKeyDelta=1 for deletes.
func validateEditNotDestructive(before, after []byte, allowKeyDelta int) error {
	oldKeys := countTopLevelKeys(before)
	newKeys := countTopLevelKeys(after)
	if newKeys < oldKeys-allowKeyDelta {
		return g.Error("destructive edit refused: top-level keys went from %d to %d", oldKeys, newKeys)
	}
	oldLines := countLines(before)
	newLines := countLines(after)
	if oldLines >= 4 && newLines*2 < oldLines {
		return g.Error("destructive edit refused: line count went from %d to %d", oldLines, newLines)
	}
	return nil
}

// setJSONPath rewrites one path; backs up and refuses destructive rewrites.
func setJSONPath(path, jsonPath string, value any) error {
	if err := backupBeforeEdit(path); err != nil {
		return err
	}
	before, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return g.Error(err, "read %s", path)
	}
	prefix, body, err := jsonReadRaw(path)
	if err != nil {
		return err
	}
	out, serr := sjson.SetBytesOptions(body, jsonPath, value, sjsonOpts)
	if serr != nil {
		return g.Error(serr, "set %s in %s", jsonPath, path)
	}
	if len(before) > 0 {
		full := append(append([]byte{}, prefix...), out...)
		if verr := validateEditNotDestructive(before, full, 0); verr != nil {
			return g.Error(verr, "would have corrupted %s — left original in place; previous content also at %s%s", path, path, backupSuffix)
		}
	}
	return jsonWriteRaw(path, prefix, out)
}

// deleteJSONPath removes one path; same backup + sanity checks as setJSONPath.
func deleteJSONPath(path, jsonPath string) error {
	if err := backupBeforeEdit(path); err != nil {
		return err
	}
	before, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return g.Error(err, "read %s", path)
	}
	prefix, body, err := jsonReadRaw(path)
	if err != nil {
		return err
	}
	out, derr := sjson.DeleteBytes(body, jsonPath)
	if derr != nil {
		return g.Error(derr, "delete %s in %s", jsonPath, path)
	}
	if len(before) > 0 {
		full := append(append([]byte{}, prefix...), out...)
		if verr := validateEditNotDestructive(before, full, 1); verr != nil {
			return g.Error(verr, "would have corrupted %s — left original in place; previous content also at %s%s", path, path, backupSuffix)
		}
	}
	return jsonWriteRaw(path, prefix, out)
}

// gjsonGetArrayStrings reads a JSON string array (JSONC-safe).
func gjsonGetArrayStrings(data []byte, path string) []string {
	clean := jsonc.ToJSON(data)
	res := gjson.GetBytes(clean, path)
	if !res.Exists() || !res.IsArray() {
		return nil
	}
	out := []string{}
	res.ForEach(func(_, v gjson.Result) bool {
		out = append(out, v.String())
		return true
	})
	return out
}
