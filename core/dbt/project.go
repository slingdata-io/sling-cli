package dbt

type MapOfMaps map[string]map[string]interface{}

type Project struct {
	Name          string                 `json:"name"`
	Version       string                 `json:"version"`
	ConfigVersion int                    `json:"config-version"`
	Profile       string                 `json:"profile"`
	SourcePaths   []string               `json:"source-paths"`
	AnalysisPaths []string               `json:"analysis-paths"`
	TestPaths     []string               `json:"test-paths"`
	DataPaths     []string               `json:"data-paths"`
	MacroPaths    []string               `json:"macro-paths"`
	SnapshotPaths []string               `json:"snapshot-paths"`
	TargetPath    string                 `json:"target-path"`
	CleanTargets  []string               `json:"clean-targets"`
	Vars          map[string]interface{} `json:"vars"`
	Models        MapOfMaps              `json:"models"`
}
