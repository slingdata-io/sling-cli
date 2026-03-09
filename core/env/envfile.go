package env

import (
	"os"
	"path"
	"sort"
	"strings"

	"github.com/flarco/g"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/samber/lo"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v2"
)

type EnvFile struct {
	Connections map[string]map[string]any `json:"connections,omitempty" yaml:"connections,omitempty"`
	Env         map[string]any            `json:"env,omitempty" yaml:"env,omitempty"`
	Variables   map[string]any            `json:"variables,omitempty" yaml:"variables,omitempty"` // legacy

	Path       string `json:"-" yaml:"-"`
	TopComment string `json:"-" yaml:"-"`
	Body       string `json:"-" yaml:"-"`
}

func (ef *EnvFile) WriteEnvFile() (err error) {
	connsMap := yaml.MapSlice{}

	// order connections names
	names := lo.Keys(ef.Connections)
	sort.Strings(names)
	for _, name := range names {
		keyMap := ef.Connections[name]
		// order connection keys (type first)
		cMap := yaml.MapSlice{}
		keys := lo.Keys(keyMap)
		sort.Strings(keys)
		if v, ok := keyMap["type"]; ok {
			cMap = append(cMap, yaml.MapItem{Key: "type", Value: v})
		}

		for _, k := range keys {
			if k == "type" {
				continue // already put first
			}
			k = cast.ToString(k)
			cMap = append(cMap, yaml.MapItem{Key: k, Value: keyMap[k]})
		}

		// add to connection map
		connsMap = append(connsMap, yaml.MapItem{Key: name, Value: cMap})
	}

	efMap := yaml.MapSlice{
		{Key: "connections", Value: connsMap},
		{Key: "variables", Value: ef.Env},
	}

	envBytes, err := yaml.Marshal(efMap)
	if err != nil {
		return g.Error(err, "could not marshal into YAML")
	}

	output := []byte(ef.TopComment + string(envBytes))

	// fix windows path
	ef.Path = strings.ReplaceAll(ef.Path, `\`, `/`)
	err = os.WriteFile(ef.Path, formatYAML(output), 0644)
	if err != nil {
		return g.Error(err, "could not write YAML file")
	}

	return
}

func formatYAML(input []byte) []byte {
	newOutput := []byte{}
	pIndent := 0
	indent := 0
	inIndent := true
	prevC := byte('-')
	for _, c := range input {
		add := false
		if c == ' ' && inIndent {
			indent++
			add = true
		} else if c == '\n' {
			pIndent = indent
			indent = 0
			add = true
			inIndent = true
		} else if prevC == '\n' {
			newOutput = append(newOutput, '\n') // add extra space
			add = true
		} else if prevC == ' ' && pIndent > indent && inIndent {
			newOutput = append(newOutput, '\n') // add extra space
			for i := 0; i < indent; i++ {
				newOutput = append(newOutput, ' ')
			}
			add = true
			inIndent = false
		} else {
			add = true
			inIndent = false
		}

		if add {
			newOutput = append(newOutput, c)
		}
		prevC = c
	}
	return newOutput
}

var dotEnvMap = cmap.New[string]()

// LoadDotEnvSling reads a `.env.sling` file from the current working directory
// and injects its key=value pairs into os environment variables.
// Existing env vars are not overwritten.
func LoadDotEnvSling() map[string]string {
	cwd, err := os.Getwd()
	if err != nil {
		return dotEnvMap.Items()
	}

	dotEnvPath := path.Join(cwd, ".env.sling")
	bytes, err := os.ReadFile(dotEnvPath)
	if err != nil {
		return dotEnvMap.Items() // file doesn't exist or can't be read
	}

	for key, val := range ParseDotEnv(string(bytes)) {
		// don't overwrite existing env vars
		if _, exists := os.LookupEnv(key); !exists {
			dotEnvMap.Set(key, val)
			os.Setenv(key, val)
		}
	}
	return dotEnvMap.Items()
}

// ParseDotEnv parses a .env file content into key-value pairs.
// It supports single-line and multi-line values enclosed in matching quotes (' or ").
func ParseDotEnv(content string) map[string]string {
	result := map[string]string{}
	lines := strings.Split(content, "\n")

	for i := 0; i < len(lines); i++ {
		line := strings.TrimSpace(lines[i])
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		key, val, found := strings.Cut(line, "=")
		if !found {
			continue
		}

		key = strings.TrimSpace(key)
		val = strings.TrimSpace(val)

		// check for quoted multi-line values
		if len(val) >= 1 && (val[0] == '\'' || val[0] == '"') {
			quote := val[0]

			// check if closing quote is on the same line
			if len(val) >= 2 && val[len(val)-1] == quote {
				// single-line quoted value
				val = val[1 : len(val)-1]
			} else {
				// multi-line: accumulate lines until we find the closing quote
				var buf strings.Builder
				buf.WriteString(val[1:]) // content after opening quote
				for i++; i < len(lines); i++ {
					raw := lines[i]
					trimmed := strings.TrimRight(raw, " \t")
					if len(trimmed) > 0 && trimmed[len(trimmed)-1] == quote {
						buf.WriteByte('\n')
						buf.WriteString(trimmed[:len(trimmed)-1])
						break
					}
					buf.WriteByte('\n')
					buf.WriteString(raw)
				}
				val = buf.String()
			}
		}

		result[key] = val
	}
	return result
}

func UnsetEnvKeys(keys []string) {
	for _, key := range keys {
		os.Unsetenv(key)
	}
}

func LoadEnvFile(path string) (ef EnvFile) {
	bytes, _ := os.ReadFile(path)

	ef.Body = string(bytes)
	ef.Path = path

	// expand variables
	envMap := map[string]any{"SLING_HOME_DIR": HomeDir}
	for _, tuple := range os.Environ() {
		key := strings.Split(tuple, "=")[0]
		val := strings.TrimPrefix(tuple, key+"=")
		envMap[key] = val
	}
	ef.Body = g.Rmd(ef.Body, envMap)

	err := yaml.Unmarshal([]byte(ef.Body), &ef)
	if err != nil {
		err = g.Error(err, "error parsing yaml string")
		_ = err
	}

	if ef.Connections == nil {
		ef.Connections = map[string]map[string]any{}
	}

	if len(ef.Env) == 0 {
		if len(ef.Variables) == 0 {
			ef.Env = map[string]any{}
		} else {
			ef.Env = ef.Variables // support legacy
		}
	}

	for k, v := range ef.Env {
		if _, found := envMap[k]; !found {
			os.Setenv(k, g.CastToString(v))
		}
	}
	return ef
}

func GetEnvFilePath(dir string) string {
	return CleanWindowsPath(path.Join(dir, "env.yaml"))
}
