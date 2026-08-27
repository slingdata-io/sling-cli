package evals

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cast"
	"gopkg.in/yaml.v3"
)

// FixtureDef is one named dataset in fixtures/registry.yaml.
type FixtureDef struct {
	Connection string       `yaml:"connection"`
	Requires   []string     `yaml:"requires"`
	Provision  []GraderSpec `yaml:"provision"`
	ReadyCheck ReadyCheck   `yaml:"ready_check"`
}

// ReadyCheck is an idempotent probe.
type ReadyCheck struct {
	Connection string `yaml:"connection"`
	SQL        string `yaml:"sql"`
	Equals     any    `yaml:"equals"`
}

// LoadFixtureRegistry reads tests/evals/fixtures/registry.yaml.
func LoadFixtureRegistry(path string) (map[string]FixtureDef, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	out := map[string]FixtureDef{}
	if err := yaml.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func registryPath() string {
	return filepath.Join(fixturesDir(), "registry.yaml")
}

func fixtureVarDir() string {
	return filepath.Join(fixturesDir(), "var")
}

func tpchDuckPath() string {
	return filepath.Join(fixtureVarDir(), "tpch.duckdb")
}

// Provisioner loads and caches named fixtures once per suite.
type Provisioner struct {
	Bin      string
	Env      []string
	Logf     func(string, ...any)
	Registry map[string]FixtureDef
	ready    map[string]bool
	skipped  map[string]string
}

func NewProvisioner(bin string, env []string, logf func(string, ...any)) (*Provisioner, error) {
	reg, err := LoadFixtureRegistry(registryPath())
	if err != nil {
		return nil, err
	}
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &Provisioner{
		Bin:      bin,
		Env:      env,
		Logf:     logf,
		Registry: reg,
		ready:    map[string]bool{},
		skipped:  map[string]string{},
	}, nil
}

func (p *Provisioner) logf(format string, args ...any) {
	if p.Logf != nil {
		p.Logf(format, args...)
	}
}

// Ensure provisions names (and requires) unless ready_check already passes.
func (p *Provisioner) Ensure(names []string) error {
	seen := map[string]bool{}
	var walk func(string) error
	walk = func(name string) error {
		if seen[name] || p.ready[name] {
			return nil
		}
		seen[name] = true
		def, ok := p.Registry[name]
		if !ok {
			return fmt.Errorf("unknown fixture %s", name)
		}
		for _, req := range def.Requires {
			if err := walk(req); err != nil {
				return err
			}
			if p.skipped[req] != "" && name != "tpch_duckdb" {
				p.skipped[name] = "requires " + req + " skipped: " + p.skipped[req]
				p.logf("fixture %s: skip (%s)", name, p.skipped[name])
				return nil
			}
		}
		if err := p.ensureOne(name, def); err != nil {
			return err
		}
		return nil
	}
	for _, n := range names {
		if err := walk(n); err != nil {
			return err
		}
	}
	return nil
}

func (p *Provisioner) ensureOne(name string, def FixtureDef) error {
	if p.ready[name] {
		return nil
	}
	if name != "tpch_duckdb" && def.Connection != "" && def.Connection != "DUCKDB" {
		if down := p.connDown(def.Connection); down {
			p.skipped[name] = "connection " + def.Connection + " down"
			p.logf("fixture %s: skip (conn down: %s)", name, def.Connection)
			return nil
		}
	}
	ok, err := p.readyCheck(def)
	if err != nil {
		p.logf("fixture %s ready_check: %v", name, err)
	}
	if ok {
		p.ready[name] = true
		p.logf("fixture %s: already ready", name)
		return nil
	}
	if name == "ecom_dirty" {
		if err := EnsureEcomParquet(); err != nil {
			return fmt.Errorf("ecom parquet: %w", err)
		}
	}
	if name == "tpch_duckdb" {
		if err := os.MkdirAll(fixtureVarDir(), 0o755); err != nil {
			return err
		}
	}
	for _, step := range def.Provision {
		cmd := cast.ToString(step["sling"])
		if cmd == "" {
			continue
		}
		if err := p.runSling(cmd); err != nil {
			if name != "tpch_duckdb" && def.Connection != "DUCKDB" {
				p.skipped[name] = err.Error()
				p.logf("fixture %s: skip (%v)", name, err)
				return nil
			}
			return fmt.Errorf("provision %s: %w", name, err)
		}
	}
	ok, err = p.readyCheck(def)
	if err != nil {
		return fmt.Errorf("fixture %s ready_check after provision: %w", name, err)
	}
	if !ok {
		return fmt.Errorf("fixture %s ready_check failed after provision", name)
	}
	p.ready[name] = true
	p.logf("fixture %s: provisioned", name)
	return nil
}

func (p *Provisioner) readyCheck(def FixtureDef) (bool, error) {
	if def.ReadyCheck.SQL == "" {
		return false, nil
	}
	conn := def.ReadyCheck.Connection
	if conn == "" {
		conn = def.Connection
	}
	out, err := p.execSQL(conn, def.ReadyCheck.SQL)
	if err != nil {
		return false, err
	}
	got := firstScalar(out)
	want := def.ReadyCheck.Equals
	if want == nil {
		return true, nil
	}
	return scalarsEqual(got, want, nil), nil
}

func (p *Provisioner) connDown(name string) bool {
	c := exec.Command(p.Bin, "conns", "test", name)
	c.Env = p.provisionEnv()
	return c.Run() != nil
}

func (p *Provisioner) provisionEnv() []string {
	envv := dropEnvKeys(append([]string{}, p.Env...), "SLING_HOME_DIR")
	home := filepath.Join(os.TempDir(), "eval-fixture-home")
	_ = os.MkdirAll(filepath.Join(home, ".sling"), 0o755)
	// Overlay host conns into a tiny env.yaml that points DUCKDB at the fixture file.
	src := filepath.Join(fixturesDir(), "home_claude", ".sling", "env.yaml")
	dst := filepath.Join(home, ".sling", "env.yaml")
	if b, err := os.ReadFile(src); err == nil {
		_ = os.WriteFile(dst, b, 0o644)
		_ = injectHostConns(dst)
		_ = patchDuckDBInstance(dst, tpchDuckPath())
	}
	envv = append(envv, "HOME="+home, "SLING_HOME_DIR="+filepath.Join(home, ".sling"))
	return envv
}

func (p *Provisioner) execSQL(conn, sql string) (string, error) {
	c := exec.Command(p.Bin, "conns", "exec", conn, sql, "-o", "csv")
	c.Env = p.provisionEnv()
	c.Dir = evalsDir()
	var stdout, stderr strings.Builder
	c.Stdout = &stdout
	c.Stderr = &stderr
	if err := c.Run(); err != nil {
		return "", fmt.Errorf("%s: %w", stderr.String(), err)
	}
	return stdout.String(), nil
}

func (p *Provisioner) runSling(cmd string) error {
	fields := splitQuoted(cmd)
	c := exec.Command(p.Bin, fields...)
	c.Env = p.provisionEnv()
	c.Dir = evalsDir()
	var buf strings.Builder
	c.Stdout = &buf
	c.Stderr = &buf
	if err := c.Run(); err != nil {
		return fmt.Errorf("%s: %w", buf.String(), err)
	}
	return nil
}

// Reset drops eval_* schemas and the local TPC-H duckdb file.
func (p *Provisioner) Reset() error {
	p.ready = map[string]bool{}
	p.skipped = map[string]string{}
	_ = os.Remove(tpchDuckPath())
	for _, conn := range []string{"POSTGRES", "CLICKHOUSE"} {
		if p.connDown(conn) {
			p.logf("reset-fixtures: skip drop on %s (down)", conn)
			continue
		}
		q := "drop schema if exists eval_tpch cascade; drop schema if exists eval_ecom cascade"
		if conn == "CLICKHOUSE" {
			q = "drop database if exists eval_tpch; drop database if exists eval_ecom"
		}
		if _, err := p.execSQL(conn, q); err != nil {
			p.logf("reset-fixtures %s: %v", conn, err)
		}
	}
	return nil
}

func (p *Provisioner) Skipped(name string) string {
	return p.skipped[name]
}

func (p *Provisioner) Ready(name string) bool {
	return p.ready[name]
}

func patchDuckDBInstance(envPath, instance string) error {
	b, err := os.ReadFile(envPath)
	if err != nil {
		return err
	}
	var doc map[string]any
	if err := yaml.Unmarshal(b, &doc); err != nil {
		return err
	}
	conns, _ := asMap(doc["connections"])
	if conns == nil {
		conns = map[string]any{}
	}
	conns["DUCKDB"] = map[string]any{"type": "duckdb", "instance": instance}
	doc["connections"] = conns
	out, err := yaml.Marshal(doc)
	if err != nil {
		return err
	}
	return os.WriteFile(envPath, out, 0o644)
}

func collectFixtureNames(cases []Case) []string {
	seen := map[string]bool{}
	var out []string
	for _, c := range cases {
		for _, f := range c.Fixtures {
			if !seen[f] {
				seen[f] = true
				out = append(out, f)
			}
		}
	}
	return out
}

// EnsureEcomParquet writes deterministic dirty ecommerce parquet if missing.
func EnsureEcomParquet() error {
	dir := filepath.Join(fixturesDir(), "data", "ecom")
	need := []string{"raw_orders.parquet", "raw_customers.parquet", "raw_events.parquet"}
	all := true
	for _, n := range need {
		if _, err := os.Stat(filepath.Join(dir, n)); err != nil {
			all = false
			break
		}
	}
	if all {
		return nil
	}
	return GenerateEcomParquet(dir)
}

// GenerateEcomParquet writes dirty ecommerce tables as parquet via DuckDB.
func GenerateEcomParquet(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	// Write CSV first (deterministic), then convert with DuckDB if sling is present.
	if err := writeEcomCSV(dir); err != nil {
		return err
	}
	bin, err := FindSlingBin()
	if err != nil {
		return fmt.Errorf("need sling to write parquet: %w", err)
	}
	db := filepath.Join(dir, "_gen.duckdb")
	_ = os.Remove(db)
	defer os.Remove(db)
	pairs := [][2]string{
		{filepath.Join(dir, "raw_orders.csv"), filepath.Join(dir, "raw_orders.parquet")},
		{filepath.Join(dir, "raw_customers.csv"), filepath.Join(dir, "raw_customers.parquet")},
		{filepath.Join(dir, "raw_events.csv"), filepath.Join(dir, "raw_events.parquet")},
	}
	home := filepath.Join(os.TempDir(), "ecom-gen-home")
	_ = os.MkdirAll(filepath.Join(home, ".sling"), 0o755)
	_ = os.WriteFile(filepath.Join(home, ".sling", "env.yaml"), []byte("connections:\n  LOCAL:\n    type: local\n    url: file://.\n"), 0o644)
	envv := append(os.Environ(), "HOME="+home, "SLING_HOME_DIR="+filepath.Join(home, ".sling"))
	for _, pair := range pairs {
		body := fmt.Sprintf("source: LOCAL\ntarget: LOCAL\nstreams:\n  \"file://%s\":\n    object: \"file://%s\"\n    mode: full-refresh\n", pair[0], pair[1])
		tmp := filepath.Join(dir, "_to_parquet.yaml")
		if err := os.WriteFile(tmp, []byte(body), 0o644); err != nil {
			return err
		}
		c := exec.Command(bin, "run", "-r", tmp)
		c.Env = envv
		c.Dir = dir
		var buf strings.Builder
		c.Stdout = &buf
		c.Stderr = &buf
		if err := c.Run(); err != nil {
			return fmt.Errorf("parquet convert %s: %s: %w", pair[1], buf.String(), err)
		}
		_ = os.Remove(tmp)
	}
	_ = db
	return nil
}

func splitQuoted(s string) []string {
	var out []string
	var cur strings.Builder
	inQ := false
	for _, r := range s {
		switch {
		case r == '"':
			inQ = !inQ
		case r == ' ' && !inQ:
			if cur.Len() > 0 {
				out = append(out, cur.String())
				cur.Reset()
			}
		default:
			cur.WriteRune(r)
		}
	}
	if cur.Len() > 0 {
		out = append(out, cur.String())
	}
	return out
}

func writeEcomCSV(dir string) error {
	// 5000 orders: duplicates, mixed date formats, nulls, mixed-case status.
	// 800 customers: messy country codes, whitespace, duplicate emails.
	// 3000 events: out-of-order timestamps.
	ord, err := os.Create(filepath.Join(dir, "raw_orders.csv"))
	if err != nil {
		return err
	}
	defer ord.Close()
	fmt.Fprintln(ord, "order_id,customer_id,status,order_date,amount,loaded_at")
	for i := 1; i <= 5000; i++ {
		status := []string{"PAID", "paid", "Shipped", "cancelled", "PENDING"}[i%5]
		var date string
		if i%2 == 0 {
			date = fmt.Sprintf("2024-%02d-%02d", 1+i%12, 1+i%28)
		} else {
			date = fmt.Sprintf("01/%02d/2024", 1+i%12)
		}
		if i%17 == 0 {
			date = ""
		}
		loaded := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(i) * time.Minute)
		id := i
		if i%40 == 0 {
			id = i - 1 // duplicate of previous order_id, older loaded_at
			loaded = loaded.Add(-time.Hour)
		}
		fmt.Fprintf(ord, "%d,%d,%s,%s,%.2f,%s\n", id, 1+(id%800), status, date, float64(10+id%90)+0.5, loaded.Format(time.RFC3339))
	}
	cus, err := os.Create(filepath.Join(dir, "raw_customers.csv"))
	if err != nil {
		return err
	}
	defer cus.Close()
	fmt.Fprintln(cus, "customer_id,email,country_code,address")
	for i := 1; i <= 800; i++ {
		email := fmt.Sprintf(" user%d@example.com ", i)
		cc := []string{"us", "USA", " gb", "DE ", "ca", "Canada"}[i%6]
		if i%50 == 0 {
			email = fmt.Sprintf("user%d@example.com", i) // duplicate email, no extra row
		}
		fmt.Fprintf(cus, "%d,%s,%s,addr-%d\n", i, email, cc, i)
	}
	ev, err := os.Create(filepath.Join(dir, "raw_events.csv"))
	if err != nil {
		return err
	}
	defer ev.Close()
	fmt.Fprintln(ev, "event_id,order_id,ts,kind")
	for i := 1; i <= 3000; i++ {
		// shuffle timestamps so they are not in order
		off := (i*7 + 13) % 3000
		ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(off) * time.Hour)
		fmt.Fprintf(ev, "%d,%d,%s,%s\n", i, 1+(i%5000), ts.Format(time.RFC3339), []string{"view", "cart", "buy"}[i%3])
	}
	return nil
}

