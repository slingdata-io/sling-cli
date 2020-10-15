package dbt

import "time"

// RunResult is a run result
type RunResult struct {
	Results     []result  `json:"results"`
	GeneratedAt time.Time `json:"generated_at"`
	ElapsedTime float64   `json:"elapsed_time"`
}

// Manifest is a manifest json file
type Manifest struct {
	Nodes       map[string]node     `json:"nodes"`
	GeneratedAt time.Time           `json:"generated_at"`
	ParentMap   map[string][]string `json:"parent_map"`
	ChildMap    map[string][]string `json:"child_map"`
}

type node struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	CompiledSQL string                 `json:"compiled_sql"`
	RawSQL      string                 `json:"raw_sql"`
	Schema      string                 `json:"schema"`
	Config      map[string]interface{} `json:"config"`
	Columns     map[string]column      `json:"columns"`
	DependsOn   depend                 `json:"depends_on"`
}

type depend struct {
	Macros []string `json:"macros"`
	Nodes  []string `json:"nodes"`
}

type column struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	Meta        map[string]interface{} `json:"meta"`
	DataType    string                 `json:"data_type"`
	Tags        []string               `json:"tags"`
}

type result struct {
	Node        node    `json:"node"`
	Error       string  `json:"error"`
	Status      string  `json:"status"`
	ElapsedTime float64 `json:"elapsed_time"`
}

// GetCompiledSQL returns a map of model compiled SQLs
func (rr RunResult) GetCompiledSQL() (sqlMap map[string]string) {
	sqlMap = map[string]string{}
	for _, result := range rr.Results {
		sqlMap[result.Node.Name] = result.Node.CompiledSQL
	}
	return
}
