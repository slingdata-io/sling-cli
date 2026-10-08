package env

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/flarco/g"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v3"
)

// ErrStaleEnvFile is returned by EnvFileEditor.Save when the file on disk
// changed since the editor was loaded (or since the caller last saved). The
// client reloads and applies its change again.
var ErrStaleEnvFile = errors.New("env.yaml changed since you loaded it; reload and apply again")

// TemplateKeyOrder, when set, returns the canonical key order of a new
// connection entry built from props: `type` first, then the template property
// order of the entry's type, then the remaining keys. core/dbio registers the
// order of core/dbio/templates/_properties.yaml; without it new entries get
// `type` first and the other keys alphabetically.
var TemplateKeyOrder func(props map[string]any) []string

// envVarRefComment is the trailing comment written next to new ${VAR} refs.
const envVarRefComment = "replace with the value, or set the env var (CI)"

// EditOptions controls EnvFileEditor.Set.
type EditOptions struct {
	// Replace treats props as the full entry: keys that are not in props are
	// removed, together with their comments. Without it the entry is merged,
	// and keys that props does not pass are kept.
	Replace bool
	// AllowOverwrite is false on create: an existing entry is an error.
	AllowOverwrite bool
	// EnvUpdates, when non-empty, are written under `env:` in the same edit.
	EnvUpdates map[string]any
	// AllowEnvOverwrite permits replacing existing env: values.
	AllowEnvOverwrite bool
}

// EnvFileEditor edits one env.yaml as text. The parsed tree only tells where
// an entry is; an edit then changes the lines of that entry and nothing else.
// Comments, blank lines, indentation, quotes and key order of all other lines
// stay byte for byte.
type EnvFileEditor struct {
	path  string
	body  []byte      // bytes as read, for Sha
	mode  os.FileMode // mode as read, restored on Save
	lines []string    // lines without their line ending
	eol   string      // "\n" or "\r\n"
	endNL bool        // the file ends with a line ending
	root  *yaml.Node
}

// LoadEnvEditor loads the file at path for editing. A missing or empty file
// yields an empty document, so a Set creates it.
func LoadEnvEditor(path string) (*EnvFileEditor, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, g.Error(err, "could not read %s", path)
		}
		body = nil
	}
	return LoadEnvEditorBytes(path, body)
}

// LoadEnvEditorBytes loads body as the content of path. The bytes do not need
// to be on disk yet; Save re-reads the disk file for the sha check.
func LoadEnvEditorBytes(path string, b []byte) (*EnvFileEditor, error) {
	e := &EnvFileEditor{path: strings.ReplaceAll(path, `\`, `/`), body: b, mode: 0o644, eol: "\n", endNL: true}
	if info, err := os.Stat(path); err == nil {
		e.mode = info.Mode().Perm()
	}

	// a repair of tab or odd-space indentation is kept: it is what makes the
	// file parse at all
	fixed := repairEnvYAML(b)
	text := string(fixed)
	if strings.Contains(text, "\r\n") {
		e.eol = "\r\n"
	}
	if text != "" {
		e.endNL = strings.HasSuffix(text, "\n")
		text = strings.TrimSuffix(text, "\n")
		e.lines = strings.Split(text, "\n")
		for i, line := range e.lines {
			e.lines[i] = strings.TrimSuffix(line, "\r")
		}
	}

	root, err := parseEnvLines(e.lines)
	if err == nil {
		err = checkEnvKeys(root)
	}
	if err != nil {
		return nil, g.Error("%s is not valid YAML. Fix it before sling changes the file: %s", path, g.ErrMsgSimple(err))
	}
	e.root = root
	return e, nil
}

// parseEnvLines parses lines as one YAML document with a mapping at the top.
// An empty or comment-only file yields an empty mapping.
func parseEnvLines(lines []string) (*yaml.Node, error) {
	root := &yaml.Node{}
	dec := yaml.NewDecoder(strings.NewReader(strings.Join(lines, "\n") + "\n"))
	if err := dec.Decode(root); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	// a second document makes the edit ambiguous: refuse it
	var extra yaml.Node
	if err := dec.Decode(&extra); err == nil && extra.Kind != 0 {
		return nil, g.Error("env.yaml has more than one YAML document; sling edits one document per file")
	}
	if root.Kind == 0 || len(root.Content) == 0 {
		return &yaml.Node{Kind: yaml.DocumentNode, Content: []*yaml.Node{{Kind: yaml.MappingNode, Tag: "!!map"}}}, nil
	}
	if root.Content[0].Kind == yaml.ScalarNode && root.Content[0].Tag == "!!null" {
		root.Content[0] = &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	}
	if root.Content[0].Kind != yaml.MappingNode {
		return nil, g.Error("env.yaml must hold one YAML mapping at the top")
	}
	return root, nil
}

// Path is the file the editor writes on Save.
func (e *EnvFileEditor) Path() string { return e.path }

// Sha returns the sha256 of the body the editor loaded.
func (e *EnvFileEditor) Sha() string { return BodySha(string(e.body)) }

// Names returns the connection names in the file, sorted.
func (e *EnvFileEditor) Names() []string {
	raw := rawConnectionsFromRoot(e.root)
	names := make([]string, 0, len(raw))
	for name := range raw {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Get returns the raw props of one connection: refs are not expanded, so a
// secret on disk stays ${VAR}. The location carries the entry line and the
// ${VAR} fields that reference an env var.
func (e *EnvFileEditor) Get(name string) (props map[string]any, loc ConnLocation, found bool) {
	loc = ConnLocation{Path: e.path, Connection: strings.ToUpper(name), Missing: []MissingRef{}}
	conns := mappingChild(e.root, "connections")
	if conns == nil {
		return nil, loc, false
	}
	keyNode, valNode := mappingChildFold(conns, name)
	if keyNode == nil {
		return nil, loc, false
	}
	loc.Line = keyNode.Line
	collectMissingRefs(valNode, "", &loc.Missing)
	props = map[string]any{}
	if err := valNode.Decode(&props); err != nil || valNode.Kind == yaml.ScalarNode {
		// a URL-string entry (`PG: postgres://...`) has one prop: the url
		props = map[string]any{"url": valNode.Value}
	}
	return props, loc, true
}

// Set creates or updates one connection entry. A new entry goes after the
// last entry of `connections:`. An update changes only the lines of the keys
// whose value changes; with opts.Replace it also removes the keys that props
// does not pass. opts.EnvUpdates go under `env:` in the same edit, so one Save
// carries the entry and its promoted secrets together. Nothing is written
// until Save runs.
func (e *EnvFileEditor) Set(name string, props map[string]any, opts EditOptions) error {
	name = strings.ToUpper(strings.TrimSpace(name))
	if name == "" {
		return g.Error("name is blank")
	}
	if props == nil {
		return g.Error("no properties provided for connection %s", name)
	}
	if err := ValidateKey(name); err != nil {
		return err
	}

	connsKey, conns := mappingChildFold(e.root, "connections")
	var entryKey, entryVal *yaml.Node
	if conns != nil && conns.Kind == yaml.MappingNode {
		entryKey, entryVal = mappingChildFold(conns, name)
	}
	if entryKey != nil && !opts.AllowOverwrite {
		return g.Error("connection %s already exists", name)
	}
	// an entry that uses YAML anchors cannot be rebuilt from a props map: the
	// anchor definitions live outside the entry
	if entryKey != nil && opts.Replace && entryUsesAnchors(entryVal) {
		return g.Error("connection %s uses YAML anchors; edit it in the raw env.yaml", name)
	}

	envKeys := []string{}
	if len(opts.EnvUpdates) > 0 {
		var err error
		if envKeys, err = e.checkEnvUpdates(opts.EnvUpdates, opts.AllowEnvOverwrite); err != nil {
			return err
		}
	}

	var expected map[string]any
	ed := &lineEdits{}
	if entryKey == nil {
		expected = props
		if err := e.addEntry(ed, connsKey, conns, name, props); err != nil {
			return err
		}
	} else {
		var err error
		if expected, err = e.updateEntry(ed, entryKey, entryVal, props, opts.Replace); err != nil {
			return err
		}
	}
	if len(opts.EnvUpdates) > 0 {
		if err := e.setEnvLines(ed, opts.EnvUpdates); err != nil {
			return err
		}
	}

	return e.apply(ed, func(before, after map[string]any) error {
		dropConn(before, name)
		got := dropConn(after, name)
		dropEnv(before, envKeys)
		dropEnv(after, envKeys)
		if !reflect.DeepEqual(before, after) {
			return g.Error("the edit changed other entries")
		}
		if gotMap := anyStringMap(got); gotMap != nil && !sameValue(expected, gotMap) {
			return g.Error("connection %s does not hold the new values", name)
		}
		return nil
	})
}

// Delete removes one connection entry and the comment right above it.
// Comments that follow the entry stay. Deleting the only entry leaves
// `connections: {}`.
func (e *EnvFileEditor) Delete(name string) error {
	return e.deleteEntry("connections", "connection", name)
}

// SetProvider creates or replaces one entry of `secret_providers:`. The
// entry holds exactly props after the edit.
func (e *EnvFileEditor) SetProvider(name string, props map[string]any) error {
	name = strings.TrimSpace(name)
	if err := ValidateKey(name); err != nil {
		return err
	}
	if len(props) == 0 {
		return g.Error("no properties provided for secret provider %s", name)
	}

	blockKey, block := mappingChildFold(e.root, "secret_providers")
	var entryKey, entryVal *yaml.Node
	if block != nil && block.Kind == yaml.MappingNode {
		entryKey, entryVal = mappingChildFold(block, name)
	}

	ed := &lineEdits{}
	if entryKey == nil {
		val, err := orderedConnNode(props)
		if err != nil {
			return g.Error(err, "could not render secret provider %s", name)
		}
		annotateRefs(val)
		if blockKey == nil {
			top := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map", Content: []*yaml.Node{strNode(name), val}}
			if err := e.addTopBlock(ed, "secret_providers", top, e.indentUnit()); err != nil {
				return err
			}
		} else if err := e.addChild(ed, blockKey, block, name, val, e.indentUnit()); err != nil {
			return err
		}
	} else if _, err := e.updateEntry(ed, entryKey, entryVal, props, true); err != nil {
		return err
	}

	return e.apply(ed, func(before, after map[string]any) error {
		dropEntry(before, "secret_providers", name)
		got := dropEntry(after, "secret_providers", name)
		if !reflect.DeepEqual(before, after) {
			return g.Error("the edit changed other entries")
		}
		if gotMap := anyStringMap(got); gotMap != nil && !sameValue(props, gotMap) {
			return g.Error("secret provider %s does not hold the new values", name)
		}
		return nil
	})
}

// DeleteProvider removes one entry of `secret_providers:`.
func (e *EnvFileEditor) DeleteProvider(name string) error {
	return e.deleteEntry("secret_providers", "secret provider", name)
}

// deleteEntry removes the entry name of a top-level block and the comment
// right above it.
func (e *EnvFileEditor) deleteEntry(blockName, noun, name string) error {
	blockKey, block := mappingChildFold(e.root, blockName)
	if block == nil || block.Kind != yaml.MappingNode {
		return g.Error("%s block not found in %s", blockName, e.path)
	}
	keyNode, _ := mappingChildFold(block, name)
	if keyNode == nil {
		return g.Error("did not find %s `%s`", noun, name)
	}

	ed := &lineEdits{}
	k, col := keyNode.Line-1, keyNode.Column-1
	start, end := e.headStart(k, col), e.valueEnd(k, col)
	// do not leave two blank lines where the entry was
	if start > 0 && isBlank(e.lines[start-1]) {
		if end+1 < len(e.lines) && isBlank(e.lines[end+1]) {
			end++
		} else if end+1 == len(e.lines) {
			start--
		}
	}
	ed.replace(start, end+1, nil)

	if len(block.Content) == 2 {
		line := e.lines[blockKey.Line-1]
		off := e.keyEnd(line, blockKey)
		ed.replace(blockKey.Line-1, blockKey.Line, []string{line[:off] + ": {}" + e.afterColonValue(line, off)})
	}

	return e.apply(ed, func(before, after map[string]any) error {
		dropEntry(before, blockName, name)
		dropEntry(after, blockName, name)
		if !reflect.DeepEqual(before, after) {
			return g.Error("the edit changed other entries")
		}
		return nil
	})
}

// Rename re-keys one entry. Only the key text changes: the entry keeps its
// position, its value and its comments. Renaming does not touch the promoted
// `env:` keys, so the ${VAR} refs of the entry stay valid.
func (e *EnvFileEditor) Rename(oldName, newName string) error {
	newName = strings.ToUpper(strings.TrimSpace(newName))
	if newName == "" {
		return g.Error("name is blank")
	}
	if err := ValidateKey(newName); err != nil {
		return err
	}

	conns := mappingChild(e.root, "connections")
	if conns == nil || conns.Kind != yaml.MappingNode {
		return g.Error("connections block not found in %s", e.path)
	}
	keyNode, _ := mappingChildFold(conns, oldName)
	if keyNode == nil {
		return g.Error("did not find connection `%s`", oldName)
	}
	if other, _ := mappingChildFold(conns, newName); other != nil && other != keyNode {
		return g.Error("connection %s already exists", newName)
	}

	k := keyNode.Line - 1
	line := e.lines[k]
	off := runeOffset(line, keyNode.Column-1)
	ed := &lineEdits{}
	ed.replace(k, k+1, []string{line[:off] + newName + line[e.keyEnd(line, keyNode):]})

	oldKey := keyNode.Value
	return e.apply(ed, func(before, after map[string]any) error {
		want := dropConn(before, oldKey)
		got := dropConn(after, newName)
		if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(want, got) {
			return g.Error("the rename changed other values")
		}
		return nil
	})
}

// SetEnv writes keys under the `env:` block (legacy `variables:` when `env:`
// is absent). Nothing is written until Save runs.
func (e *EnvFileEditor) SetEnv(updates map[string]any, allowOverwrite bool) error {
	if len(updates) == 0 {
		return nil
	}
	envKeys, err := e.checkEnvUpdates(updates, allowOverwrite)
	if err != nil {
		return err
	}
	ed := &lineEdits{}
	if err := e.setEnvLines(ed, updates); err != nil {
		return err
	}
	return e.apply(ed, func(before, after map[string]any) error {
		dropEnv(before, envKeys)
		dropEnv(after, envKeys)
		if !reflect.DeepEqual(before, after) {
			return g.Error("the edit changed other entries")
		}
		return nil
	})
}

// Bytes returns the edited file.
func (e *EnvFileEditor) Bytes() ([]byte, error) {
	if len(e.lines) == 0 {
		return []byte{}, nil
	}
	out := strings.Join(e.lines, e.eol)
	if e.endNL {
		out += e.eol
	}
	return []byte(out), nil
}

// Save replaces the file atomically: the bytes land in a temp file in the
// same folder, are fsynced, get the mode of the original file, and rename over
// it. A non-empty expectSha must match the sha256 of the file as it is on disk
// right now, or the save fails with ErrStaleEnvFile and the file is left
// untouched.
func (e *EnvFileEditor) Save(expectSha string) error {
	if e.path == "" {
		return g.Error("env file path is not set")
	}
	if expectSha != "" {
		current, err := os.ReadFile(e.path)
		if err != nil && !os.IsNotExist(err) {
			return g.Error(err, "could not read %s", e.path)
		}
		if BodySha(string(current)) != expectSha {
			return ErrStaleEnvFile
		}
	}

	data, err := e.Bytes()
	if err != nil {
		return err
	}
	if err := writeFileAtomic(e.path, data, e.mode); err != nil {
		return err
	}
	e.body = data
	return nil
}

// apply runs ed on the lines, then parses the result. check compares the
// decoded file before and after the edit; the edit is dropped when the result
// does not parse or check fails, so a bad splice never reaches the disk.
func (e *EnvFileEditor) apply(ed *lineEdits, check func(before, after map[string]any) error) error {
	lines := ed.apply(e.lines)
	root, err := parseEnvLines(lines)
	if err == nil {
		err = checkEnvKeys(root)
	}
	if err == nil {
		before, after := map[string]any{}, map[string]any{}
		if err = e.root.Decode(&before); err == nil {
			if err = root.Decode(&after); err == nil {
				err = check(before, after)
			}
		}
	}
	if err != nil {
		return g.Error("could not edit %s safely, the file is not changed: %s", e.path, g.ErrMsgSimple(err))
	}
	if len(e.lines) == 0 {
		e.endNL = true
	}
	e.lines, e.root = lines, root
	return nil
}

// addEntry adds a new connection after the last entry of `connections:`,
// or adds the `connections:` block at the end of the file.
func (e *EnvFileEditor) addEntry(ed *lineEdits, connsKey, conns *yaml.Node, name string, props map[string]any) error {
	var val *yaml.Node
	var err error
	if urlOnly(props) {
		// a lone `url` value keeps the URL-string form: `PG: postgres://...`
		val = &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: castString(props["url"])}
	} else if val, err = orderedConnNode(props); err != nil {
		return g.Error(err, "could not render connection %s", name)
	}
	annotateRefs(val)

	unit := e.indentUnit()
	if connsKey == nil {
		block := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map", Content: []*yaml.Node{strNode(name), val}}
		return e.addTopBlock(ed, "connections", block, unit)
	}
	return e.addChild(ed, connsKey, conns, name, val, unit)
}

// addChild appends key: val as the last child of the mapping parent, the
// value of parentKey. An empty (`key:` or `key: {}`) value becomes a block.
func (e *EnvFileEditor) addChild(ed *lineEdits, parentKey, parent *yaml.Node, key string, val *yaml.Node, unit int) error {
	k, kcol := parentKey.Line-1, parentKey.Column-1

	switch {
	case isBlockMapping(parent, parentKey):
		col := parent.Content[0].Column - 1
		lines, err := renderPair(key, val, col, unit)
		if err != nil {
			return err
		}
		if e.spacedPairs(parent) {
			lines = append([]string{""}, lines...)
		}
		ed.insert(e.blockEnd(k, kcol)+1, lines)
	case isEmptyValue(parent):
		// `key:` or `key: {}` becomes `key:` with a block under it
		line := e.lines[k]
		off := e.keyEnd(line, parentKey)
		ed.replace(k, k+1, []string{line[:off] + ":" + e.afterColonValue(line, off)})
		lines, err := renderPair(key, val, kcol+unit, unit)
		if err != nil {
			return err
		}
		ed.insert(e.blockEnd(k, kcol)+1, lines)
	case parent.Kind == yaml.MappingNode:
		// a flow mapping with entries: render the block again
		m := map[string]any{}
		if err := parent.Decode(&m); err != nil {
			return err
		}
		node, err := anyToNode(m)
		if err != nil {
			return err
		}
		node.Content = append(node.Content, strNode(key), val)
		lines, err := renderPair(parentKey.Value, node, kcol, unit)
		if err != nil {
			return err
		}
		ed.replace(k, e.valueEnd(k, kcol)+1, lines)
	default:
		return g.Error("`%s` in %s is not a mapping", parentKey.Value, e.path)
	}
	return nil
}

// addTopBlock appends `key:` with block at the end of the file.
func (e *EnvFileEditor) addTopBlock(ed *lineEdits, key string, block *yaml.Node, unit int) error {
	lines, err := renderPair(key, block, 0, unit)
	if err != nil {
		return err
	}
	last := len(e.lines)
	for last > 0 && isBlank(e.lines[last-1]) {
		last--
	}
	if last > 0 && last == len(e.lines) && e.spacedPairs(mappingRoot(e.root)) {
		lines = append([]string{""}, lines...)
	}
	ed.insert(len(e.lines), lines)
	return nil
}

// updateEntry edits one existing connection entry and returns the props it
// must hold after the edit.
func (e *EnvFileEditor) updateEntry(ed *lineEdits, key, val *yaml.Node, props map[string]any, replace bool) (map[string]any, error) {
	k, kcol := key.Line-1, key.Column-1
	unit := e.indentUnit()

	if isBlockMapping(val, key) {
		current := map[string]any{}
		if err := val.Decode(&current); err != nil {
			return nil, err
		}
		expected := props
		if !replace {
			expected = mergeProps(current, props)
		}
		m := &mappingEdit{e: e, ed: ed, unit: unit, conn: true, nestedReplace: replace}
		if err := m.set(key, val, props, connKeyOrder(expected), replace); err != nil {
			return nil, err
		}
		return expected, nil
	}

	if val.Kind == yaml.ScalarNode && val.Value != "" && urlOnly(props) {
		// editing a URL entry with its url keeps the one-line form
		if ok := e.editScalar(ed, key, val, props["url"], false); ok {
			return nil, nil
		}
	}

	// a URL entry that gets more props, an empty entry, or a flow mapping:
	// render the whole entry again
	expected := props
	if !replace {
		current := map[string]any{}
		if val.Kind == yaml.ScalarNode && val.Value != "" {
			current["url"] = val.Value
		} else if val.Kind == yaml.MappingNode {
			if err := val.Decode(&current); err != nil {
				return nil, err
			}
		}
		expected = mergeProps(current, props)
	}
	node, err := orderedConnNode(expected)
	if err != nil {
		return nil, err
	}
	if val.Kind == yaml.MappingNode && val.Style&yaml.FlowStyle != 0 {
		node.Style = yaml.FlowStyle
	} else {
		annotateRefs(node)
	}
	lines, err := renderPair(key.Value, node, kcol, unit)
	if err != nil {
		return nil, err
	}
	ed.replace(k, e.valueEnd(k, kcol)+1, lines)
	return expected, nil
}

// checkEnvUpdates validates the env keys and refuses to change an existing
// value unless allowOverwrite. It returns the keys, sorted.
func (e *EnvFileEditor) checkEnvUpdates(updates map[string]any, allowOverwrite bool) ([]string, error) {
	keys := make([]string, 0, len(updates))
	for k := range updates {
		if err := ValidateEnvKey(k); err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if allowOverwrite {
		return keys, nil
	}
	block := mappingChild(e.root, effectiveEnvKey(e.root))
	current := map[string]any{}
	if block != nil && block.Kind == yaml.MappingNode {
		_ = block.Decode(&current)
	}
	for _, k := range keys {
		if cur, ok := current[k]; ok && !sameValue(cur, updates[k]) {
			return nil, g.Error("env var %s already exists in env.yaml; pass allow_overwrite to update it", k)
		}
	}
	return keys, nil
}

// setEnvLines writes updates under the effective env block. A map value
// replaces the whole value of its key.
func (e *EnvFileEditor) setEnvLines(ed *lineEdits, updates map[string]any) error {
	blockKey := effectiveEnvKey(e.root)
	keyNode, block := mappingChildFold(e.root, blockKey)
	unit := e.indentUnit()

	if keyNode == nil {
		node, err := anyToNode(updates)
		if err != nil {
			return err
		}
		return e.addTopBlock(ed, blockKey, node, unit)
	}
	if isBlockMapping(block, keyNode) {
		m := &mappingEdit{e: e, ed: ed, unit: unit, nestedReplace: true}
		return m.set(keyNode, block, updates, sortedKeys(updates), false)
	}
	if block.Kind != yaml.MappingNode && !isEmptyValue(block) {
		return g.Error("`%s` in %s is not a mapping", keyNode.Value, e.path)
	}

	// an empty block (`env:` or `env: {}`) or a flow mapping: render the
	// block again with the updates
	current := map[string]any{}
	if block.Kind == yaml.MappingNode {
		if err := block.Decode(&current); err != nil {
			return err
		}
	}
	node, err := anyToNode(mergeTop(current, updates))
	if err != nil {
		return err
	}
	k, kcol := keyNode.Line-1, keyNode.Column-1
	lines, err := renderPair(keyNode.Value, node, kcol, unit)
	if err != nil {
		return err
	}
	line := e.lines[k]
	off := e.keyEnd(line, keyNode)
	if isEmptyValue(block) {
		ed.replace(k, k+1, []string{line[:off] + ":" + e.afterColonValue(line, off)})
		ed.insert(e.blockEnd(k, kcol)+1, lines[1:])
		return nil
	}
	ed.replace(k, e.valueEnd(k, kcol)+1, lines)
	return nil
}

// mappingEdit changes the keys of one block mapping in place.
type mappingEdit struct {
	e    *EnvFileEditor
	ed   *lineEdits
	unit int
	// conn is true inside a connection entry: new ${VAR} refs get a comment
	conn bool
	// nestedReplace makes a map value replace the nested mapping, not merge it
	nestedReplace bool
}

// set applies props to the block mapping m, the value of parentKey. Keys are
// visited in order; with drop, keys of m that props does not pass go away.
func (x *mappingEdit) set(parentKey, m *yaml.Node, props map[string]any, order []string, drop bool) error {
	e := x.e
	col := m.Content[0].Column - 1
	current := map[string]any{}
	if err := m.Decode(&current); err != nil {
		return err
	}

	var added []string
	for _, key := range order {
		newVal, ok := props[key]
		if !ok {
			continue
		}
		kn, vn := mappingChildExact(m, key)
		if kn == nil {
			// a key that a merge key (<<: *base) already gives needs no line
			if cur, ok := current[key]; ok && sameValue(cur, newVal) {
				continue
			}
			added = append(added, key)
			continue
		}
		var cur any
		if err := vn.Decode(&cur); err == nil && sameValue(cur, newVal) {
			continue
		}
		if nested := anyStringMap(newVal); nested != nil && isBlockMapping(vn, kn) {
			if err := x.set(kn, vn, nested, sortedKeys(nested), x.nestedReplace); err != nil {
				return err
			}
			continue
		}
		if e.editScalar(x.ed, kn, vn, newVal, x.conn) {
			continue
		}
		if nested := anyStringMap(newVal); nested != nil && !x.nestedReplace {
			if cm := anyStringMap(cur); cm != nil {
				newVal = mergeProps(cm, nested)
			}
		}
		node, err := anyToNode(newVal)
		if err != nil {
			return err
		}
		if vn.Style&yaml.FlowStyle != 0 && (node.Kind == yaml.MappingNode || node.Kind == yaml.SequenceNode) {
			node.Style = yaml.FlowStyle
		} else if x.conn {
			annotateRefs(node)
		}
		lines, err := renderPair(key, node, col, x.unit)
		if err != nil {
			return err
		}
		k := kn.Line - 1
		x.ed.replace(k, e.valueEnd(k, col)+1, lines)
	}

	if drop {
		for i := 0; i < len(m.Content)-1; i += 2 {
			kn := m.Content[i]
			if _, ok := props[kn.Value]; ok || kn.Value == "<<" {
				continue
			}
			k := kn.Line - 1
			x.ed.replace(e.headStart(k, col), e.valueEnd(k, col)+1, nil)
		}
	}

	if len(added) > 0 {
		var lines []string
		for _, key := range added {
			node, err := anyToNode(props[key])
			if err != nil {
				return err
			}
			if x.conn {
				annotateRefs(node)
			}
			pair, err := renderPair(key, node, col, x.unit)
			if err != nil {
				return err
			}
			lines = append(lines, pair...)
		}
		x.ed.insert(e.blockEnd(parentKey.Line-1, parentKey.Column-1)+1, lines)
	}
	return nil
}

// editScalar replaces the scalar vn on the line of kn with newVal, keeping
// the quote style and the text after the value (the spaces and the comment).
// It returns false when that is not possible: vn or newVal is not a one-line
// scalar.
func (e *EnvFileEditor) editScalar(ed *lineEdits, kn, vn *yaml.Node, newVal any, annotate bool) bool {
	if vn.Kind != yaml.ScalarNode || vn.Line != kn.Line || vn.Anchor != "" || vn.Value == "" ||
		vn.Style&(yaml.LiteralStyle|yaml.FoldedStyle) != 0 {
		return false
	}
	k := kn.Line - 1
	if e.valueEnd(k, kn.Column-1) != k {
		return false // the scalar goes on over more lines
	}
	line := e.lines[k]
	off := runeOffset(line, vn.Column-1)
	if off >= len(line) || line[off] == '!' || line[off] == '&' || line[off] == '*' {
		return false
	}
	end := scalarEnd(line, off, vn.Style)
	if end < 0 {
		return false
	}
	text, ok := scalarText(newVal, vn)
	if !ok {
		return false
	}
	rest := line[end:]
	if annotate && IsEnvVarRef(cast.ToString(newVal)) && !strings.Contains(rest, "#") {
		rest += " # " + envVarRefComment
	}
	ed.replace(k, k+1, []string{line[:off] + text + rest})
	return true
}

// keyEnd returns the byte offset just past the text of the key kn in line.
func (e *EnvFileEditor) keyEnd(line string, kn *yaml.Node) int {
	off := runeOffset(line, kn.Column-1)
	if kn.Style&(yaml.DoubleQuotedStyle|yaml.SingleQuotedStyle) != 0 {
		if end := scalarEnd(line, off, kn.Style); end > 0 {
			return end
		}
	}
	if i := strings.Index(line[off:], ":"); i >= 0 {
		return off + i
	}
	return len(line)
}

// afterColonValue returns what follows the empty value of a `key:` line: the
// spaces and the comment. off is where the key text ends. An empty flow
// mapping (`{}`) is dropped.
func (e *EnvFileEditor) afterColonValue(line string, off int) string {
	rest := strings.TrimPrefix(line[off:], ":")
	trimmed := strings.TrimLeft(rest, " \t")
	if strings.HasPrefix(trimmed, "{}") || strings.HasPrefix(trimmed, "~") || strings.HasPrefix(trimmed, "null") {
		word := "{}"
		if !strings.HasPrefix(trimmed, "{}") {
			word = strings.Fields(trimmed)[0]
		}
		rest = strings.TrimPrefix(trimmed, word)
	}
	if strings.TrimSpace(rest) == "" {
		return ""
	}
	return rest
}

// valueEnd returns the last content line of the pair whose key is on line k
// at column col: the lines after k that are indented deeper, or are `- `
// items at col, belong to it. Comments after the last content line do not.
func (e *EnvFileEditor) valueEnd(k, col int) int {
	last := k
	for i := k + 1; i < len(e.lines); i++ {
		line := e.lines[i]
		if isBlank(line) || isComment(line) {
			continue
		}
		ind := indentOf(line)
		if ind > col || (ind == col && isSeqItem(line)) {
			last = i
			continue
		}
		break
	}
	return last
}

// blockEnd is valueEnd plus the comments after it that are indented deeper
// than col: new children of the pair go after that line.
func (e *EnvFileEditor) blockEnd(k, col int) int {
	last := e.valueEnd(k, col)
	for i := last + 1; i < len(e.lines); i++ {
		line := e.lines[i]
		if isBlank(line) {
			continue
		}
		if isComment(line) && indentOf(line) > col {
			last = i
			continue
		}
		break
	}
	return last
}

// headStart returns the first line of the comment block right above the key
// on line k (comments at the same column, no blank line between).
func (e *EnvFileEditor) headStart(k, col int) int {
	i := k - 1
	for i >= 0 && isComment(e.lines[i]) && indentOf(e.lines[i]) == col {
		i--
	}
	return i + 1
}

// spacedPairs is true when a blank line separates the pairs of m, so a new
// pair gets one too.
func (e *EnvFileEditor) spacedPairs(m *yaml.Node) bool {
	if m == nil {
		return false
	}
	for i := 2; i < len(m.Content)-1; i += 2 {
		kn := m.Content[i]
		if s := e.headStart(kn.Line-1, kn.Column-1); s > 0 && isBlank(e.lines[s-1]) {
			return true
		}
	}
	return false
}

// indentUnit returns the indentation step of the file: the column step from
// the first block mapping to its first key. The default is 2.
func (e *EnvFileEditor) indentUnit() int {
	root := mappingRoot(e.root)
	if root == nil {
		return 2
	}
	var find func(m *yaml.Node) int
	find = func(m *yaml.Node) int {
		for i := 0; i < len(m.Content)-1; i += 2 {
			kn, vn := m.Content[i], m.Content[i+1]
			if isBlockMapping(vn, kn) {
				if step := vn.Content[0].Column - kn.Column; step > 0 {
					return step
				}
			}
		}
		return 0
	}
	if step := find(root); step > 0 {
		return min(max(step, 2), 9)
	}
	return 2
}

// lineEdits collects line replacements computed from one parse. They apply
// from the bottom up, so the line numbers of one edit do not shift another.
type lineEdits struct {
	edits []lineEdit
}

type lineEdit struct {
	start, end int // replace lines[start:end]; start == end inserts
	lines      []string
	seq        int
}

func (s *lineEdits) replace(start, end int, lines []string) {
	s.edits = append(s.edits, lineEdit{start: start, end: end, lines: lines, seq: len(s.edits)})
}

func (s *lineEdits) insert(at int, lines []string) {
	s.replace(at, at, lines)
}

// apply returns a copy of lines with the edits. Inserts at the same line keep
// the order in which they were added.
func (s *lineEdits) apply(lines []string) []string {
	edits := append([]lineEdit(nil), s.edits...)
	sort.SliceStable(edits, func(a, b int) bool {
		if edits[a].start != edits[b].start {
			return edits[a].start > edits[b].start
		}
		return edits[a].seq > edits[b].seq
	})
	out := append([]string(nil), lines...)
	for _, ed := range edits {
		tail := append([]string(nil), out[ed.end:]...)
		out = append(append(out[:ed.start], ed.lines...), tail...)
	}
	return out
}

// renderPair encodes `key: val` as block YAML at column col.
func renderPair(key string, val *yaml.Node, col, unit int) ([]string, error) {
	m := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map", Content: []*yaml.Node{strNode(key), val}}
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(unit)
	if err := enc.Encode(m); err != nil {
		_ = enc.Close()
		return nil, g.Error(err, "could not render %s", key)
	}
	if err := enc.Close(); err != nil {
		return nil, g.Error(err, "could not render %s", key)
	}
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	pad := strings.Repeat(" ", col)
	for i, line := range lines {
		if line != "" {
			lines[i] = pad + line
		}
	}
	return lines, nil
}

// scalarText renders v as a one-line scalar in the style of old: quotes stay
// quotes, and a plain number or bool stays plain when v is its string form.
func scalarText(v any, old *yaml.Node) (string, bool) {
	node, err := anyToNode(v)
	if err != nil || node.Kind != yaml.ScalarNode {
		return "", false
	}
	if s, isStr := v.(string); isStr {
		switch {
		case old.Style&yaml.DoubleQuotedStyle != 0:
			node.Style = yaml.DoubleQuotedStyle
		case old.Style&yaml.SingleQuotedStyle != 0:
			node.Style = yaml.SingleQuotedStyle
		case old.Style == 0 && old.ShortTag() != "!!str":
			var probe yaml.Node
			if yaml.Unmarshal([]byte(s), &probe) == nil && len(probe.Content) == 1 {
				p := probe.Content[0]
				if p.Kind == yaml.ScalarNode && p.Style == 0 && p.Value == s && p.ShortTag() == old.ShortTag() {
					node.Style, node.Tag = 0, old.ShortTag()
				}
			}
		}
	}
	b, err := yaml.Marshal(node)
	if err != nil {
		return "", false
	}
	text := strings.TrimSuffix(string(b), "\n")
	if text == "" || strings.Contains(text, "\n") || text[0] == '|' || text[0] == '>' {
		return "", false
	}
	return text, true
}

// scalarEnd returns the byte offset just past the scalar that starts at off in
// line, or -1 when a quoted scalar does not close on this line.
func scalarEnd(line string, off int, style yaml.Style) int {
	switch {
	case style&yaml.DoubleQuotedStyle != 0:
		for i := off + 1; i < len(line); i++ {
			if line[i] == '\\' {
				i++
				continue
			}
			if line[i] == '"' {
				return i + 1
			}
		}
		return -1
	case style&yaml.SingleQuotedStyle != 0:
		for i := off + 1; i < len(line); i++ {
			if line[i] == '\'' {
				if i+1 < len(line) && line[i+1] == '\'' {
					i++
					continue
				}
				return i + 1
			}
		}
		return -1
	}
	end := len(line)
	for i := off + 1; i < len(line); i++ {
		if line[i] == '#' && (line[i-1] == ' ' || line[i-1] == '\t') {
			end = i
			break
		}
	}
	return off + len(strings.TrimRight(line[off:end], " \t"))
}

// runeOffset converts a 0-based character column (as yaml.v3 counts) to a
// byte offset in line.
func runeOffset(line string, col int) int {
	off := 0
	for i := 0; i < col && off < len(line); i++ {
		_, size := utf8.DecodeRuneInString(line[off:])
		off += size
	}
	return off
}

func isBlank(line string) bool { return strings.TrimSpace(line) == "" }

func isComment(line string) bool { return strings.HasPrefix(strings.TrimSpace(line), "#") }

func isSeqItem(line string) bool {
	t := strings.TrimSpace(line)
	return t == "-" || strings.HasPrefix(t, "- ")
}

func indentOf(line string) int { return len(line) - len(strings.TrimLeft(line, " ")) }

// isBlockMapping is true when val is a non-empty block mapping under key.
func isBlockMapping(val, key *yaml.Node) bool {
	return val != nil && val.Kind == yaml.MappingNode && val.Style&yaml.FlowStyle == 0 &&
		len(val.Content) > 0 && val.Content[0].Line > key.Line
}

// isEmptyValue is true for `key:`, `key: ~`, `key: null` and `key: {}`.
func isEmptyValue(val *yaml.Node) bool {
	if val.Kind == yaml.MappingNode {
		return len(val.Content) == 0
	}
	return val.Kind == yaml.ScalarNode && val.ShortTag() == "!!null"
}

func strNode(s string) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: s}
}

func mappingChildExact(m *yaml.Node, key string) (keyNode, valNode *yaml.Node) {
	for i := 0; i < len(m.Content)-1; i += 2 {
		if m.Content[i].Value == key {
			return m.Content[i], m.Content[i+1]
		}
	}
	return nil, nil
}

// annotateRefs writes envVarRefComment on the ${VAR} scalars of a new node
// that have no comment.
func annotateRefs(n *yaml.Node) {
	switch n.Kind {
	case yaml.ScalarNode:
		if IsEnvVarRef(n.Value) && strings.TrimSpace(n.LineComment) == "" {
			n.LineComment = envVarRefComment
		}
	case yaml.MappingNode:
		for i := 1; i < len(n.Content); i += 2 {
			annotateRefs(n.Content[i])
		}
	}
}

// sameValue compares decoded YAML values loosely: scalars by their string
// form, so 5432 and "5432" are the same value.
func sameValue(a, b any) bool {
	if am, bm := anyStringMap(a), anyStringMap(b); am != nil || bm != nil {
		if am == nil || bm == nil || len(am) != len(bm) {
			return false
		}
		for k, av := range am {
			bv, ok := bm[k]
			if !ok || !sameValue(av, bv) {
				return false
			}
		}
		return true
	}
	as, aIsList := a.([]any)
	bs, bIsList := b.([]any)
	if aIsList || bIsList {
		if !aIsList || !bIsList || len(as) != len(bs) {
			return false
		}
		for i := range as {
			if !sameValue(as[i], bs[i]) {
				return false
			}
		}
		return true
	}
	return scalarString(a) == scalarString(b)
}

// scalarString is the string form of a decoded scalar. A timestamp gets the
// form YAML reads it from, so `2024-01-01` equals "2024-01-01".
func scalarString(v any) string {
	if t, ok := v.(time.Time); ok {
		if t.Equal(t.Truncate(24 * time.Hour)) {
			return t.Format("2006-01-02")
		}
		return t.Format(time.RFC3339Nano)
	}
	return cast.ToString(v)
}

// dropConn removes the connection name (any case) from a decoded file and
// returns its value.
func dropConn(file map[string]any, name string) any {
	return dropEntry(file, "connections", name)
}

// dropEntry removes name from the top-level block of file and returns its value.
func dropEntry(file map[string]any, block, name string) any {
	entries := anyStringMap(file[block])
	var val any
	for k, v := range entries {
		if strings.EqualFold(k, name) {
			val = v
			delete(entries, k)
		}
	}
	if len(entries) == 0 {
		delete(file, block)
	} else {
		file[block] = entries
	}
	return val
}

// dropEnv removes keys from the env blocks of a decoded file.
func dropEnv(file map[string]any, keys []string) {
	for _, block := range []string{"env", "variables"} {
		m := anyStringMap(file[block])
		if m == nil {
			if v, ok := file[block]; ok && v == nil {
				delete(file, block)
			}
			continue
		}
		for _, k := range keys {
			delete(m, k)
		}
		if len(m) == 0 {
			delete(file, block)
		} else {
			file[block] = m
		}
	}
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// mergeTop copies current and sets updates over it, one level deep.
func mergeTop(current, updates map[string]any) map[string]any {
	out := make(map[string]any, len(current)+len(updates))
	for k, v := range current {
		out[k] = v
	}
	for k, v := range updates {
		out[k] = v
	}
	return out
}

// writeFileAtomic writes data to path through a temp file in the same folder:
// write, fsync, chmod to mode, rename. A failure removes the temp file and
// leaves the original untouched.
func writeFileAtomic(path string, data []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return g.Error(err, "could not create temp file in %s", dir)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = tmp.Close(); _ = os.Remove(tmpName) }
	if _, err := tmp.Write(data); err != nil {
		cleanup()
		return g.Error(err, "could not write %s", tmpName)
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return g.Error(err, "could not sync %s", tmpName)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return g.Error(err, "could not close %s", tmpName)
	}
	if err := os.Chmod(tmpName, mode); err != nil {
		_ = os.Remove(tmpName)
		return g.Error(err, "could not set mode on %s", tmpName)
	}
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return g.Error(err, "could not replace %s", path)
	}
	return nil
}

// entryUsesAnchors reports whether a node tree uses aliases or merge keys,
// which a props-map rebuild cannot preserve.
func entryUsesAnchors(n *yaml.Node) bool {
	if n == nil {
		return false
	}
	switch n.Kind {
	case yaml.AliasNode:
		return true
	case yaml.MappingNode:
		for i := 0; i < len(n.Content)-1; i += 2 {
			if n.Content[i].Value == "<<" {
				return true
			}
		}
	}
	for _, child := range n.Content {
		if entryUsesAnchors(child) {
			return true
		}
	}
	return false
}

// urlOnly reports whether props is exactly one `url` entry.
func urlOnly(props map[string]any) bool {
	if len(props) != 1 {
		return false
	}
	_, ok := props["url"]
	return ok
}

func castString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// connKeyOrder returns the keys of props in the canonical order of a
// connection entry: `type`, the template order of the type (when registered),
// then the remaining keys alphabetically.
func connKeyOrder(props map[string]any) []string {
	order := []string{}
	seen := map[string]bool{}
	add := func(k string) {
		if _, ok := props[k]; ok && !seen[k] {
			seen[k] = true
			order = append(order, k)
		}
	}
	add("type")
	if TemplateKeyOrder != nil {
		for _, k := range TemplateKeyOrder(props) {
			add(k)
		}
	}
	for _, k := range sortedKeys(props) {
		add(k)
	}
	return order
}

// orderedConnNode renders props as a mapping node with the keys in
// connKeyOrder.
func orderedConnNode(props map[string]any) (*yaml.Node, error) {
	node, err := anyToNode(props)
	if err != nil {
		return nil, err
	}
	if node.Kind != yaml.MappingNode {
		return node, nil
	}
	return reorderMapping(node, connKeyOrder(props)), nil
}

// mergeProps copies existing and applies incoming; nested maps merge. Keys
// that incoming does not pass stay on the result (the CLI contract).
func mergeProps(existing, incoming map[string]any) map[string]any {
	out := make(map[string]any, len(existing)+len(incoming))
	for k, v := range existing {
		out[k] = v
	}
	for k, v := range incoming {
		if vm := anyStringMap(v); vm != nil {
			if em := anyStringMap(out[k]); em != nil {
				out[k] = mergeProps(em, vm)
				continue
			}
		}
		out[k] = v
	}
	return out
}

// anyStringMap returns m as a map[string]any when it holds a mapping.
func anyStringMap(v any) map[string]any {
	switch m := v.(type) {
	case map[string]any:
		return m
	case map[any]any:
		out := make(map[string]any, len(m))
		for k, item := range m {
			out[cast.ToString(k)] = item
		}
		return out
	}
	return nil
}

// reorderMapping re-orders a mapping node: keys named in order keep that
// order, keys that are not stay in their original relative order.
func reorderMapping(node *yaml.Node, order []string) *yaml.Node {
	rank := make(map[string]int, len(order))
	for i, k := range order {
		rank[k] = i
	}
	type pair struct{ key, val *yaml.Node }
	pairs := make([]pair, 0, len(node.Content)/2)
	for i := 0; i < len(node.Content)-1; i += 2 {
		pairs = append(pairs, pair{node.Content[i], node.Content[i+1]})
	}
	sort.SliceStable(pairs, func(a, b int) bool {
		ra, oka := rank[pairs[a].key.Value]
		rb, okb := rank[pairs[b].key.Value]
		if oka && okb {
			return ra < rb
		}
		if oka != okb {
			return oka
		}
		return false
	})
	content := make([]*yaml.Node, 0, len(pairs)*2)
	for _, p := range pairs {
		content = append(content, p.key, p.val)
	}
	node.Content = content
	return node
}
