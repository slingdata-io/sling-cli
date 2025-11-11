package env

import (
	"os"
	"path"
	"sort"
	"strings"

	"github.com/flarco/g"
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

func SetHomeDir(name string) string {
	envKey := strings.ToUpper(name) + "_HOME_DIR"
	dir := os.Getenv(envKey)
	if dir == "" {
		dir = path.Join(g.UserHomeDir(), "."+name)
		os.Setenv(envKey, dir)
	}
	envMux.Lock()
	HomeDirs[name] = dir
	envMux.Unlock()
	return dir
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

func LoadEnvFile(path string) (ef EnvFile) {
	bytes, _ := os.ReadFile(path)

	ef.Body = string(bytes)
	ef.Path = path

	// expand variables
	envMap := map[string]any{}
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

	if ef.Env == nil {
		if ef.Variables == nil {
			ef.Env = map[string]any{}
		} else {
			ef.Env = ef.Variables // support legacy
		}
	}

	for k, v := range ef.Env {
		if _, found := envMap[k]; !found {
			os.Setenv(k, cast.ToString(v))
		}
	}
	return ef
}

func GetEnvFilePath(dir string) string {
	return path.Join(dir, "env.yaml")
}
