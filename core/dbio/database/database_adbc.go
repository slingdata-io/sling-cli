package database

import (
	"archive/tar"
	"archive/zip"
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/flarco/g"
	"github.com/flarco/g/net"
	"github.com/klauspost/compress/zstd"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
)

// ArrowDBConn is an Arrow FlightSQL connection
type ArrowDBConn struct {
	BaseConn
	URL        string
	db         adbc.Database
	Conn       adbc.Connection
	driverType dbio.Type // Underlying database type for templates
}

// Init initiates the connection
func (conn *ArrowDBConn) Init() error {
	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbArrowDBC
	conn.BaseConn.defaultPort = 12345

	// Build ADBC-specific properties map
	// Filter out sling-specific properties and only pass ADBC driver properties
	adbcProps := map[string]string{}

	// List of sling-specific keys to exclude
	excludeKeys := map[string]bool{
		"type":           true,
		"driver_name":    true,
		"orig_prop_keys": true,
		"name":           true,
		"conn":           true,
		"database":       true,
		"schema":         true,
		"url":            true,
	}

	// Property mappings from sling format to ADBC driver format
	// Different drivers expect different property names
	propertyMappings := map[string]string{
		"adbc.postgresql.connection_string": "uri",
		"adbc.sqlserver.connection_string":  "uri",
		"adbc.mssql.connection_string":      "uri",
		"adbc.snowflake.connection_string":  "uri",
		"adbc.sqlite.connection_string":     "uri",
		"adbc.duckdb.connection_string":     "path",
		"adbc.mysql.connection_string":      "uri",
		"adbc.trino.connection_string":      "uri",
	}

	for key, val := range conn.properties {
		// Skip excluded keys
		if excludeKeys[key] {
			continue
		}

		// Check if there's a property mapping
		if mappedKey, ok := propertyMappings[key]; ok {
			adbcProps[mappedKey] = val
			continue
		}

		// Include driver property and any adbc.* prefixed properties
		if key == "driver" || key == "driver_entrypoint" || key == "uri" || strings.HasPrefix(key, "adbc.") {
			adbcProps[key] = val
		}
	}

	// Resolve driver path if not explicitly provided
	if adbcProps["driver"] == "" {
		driverPath := conn.resolveDriverPath()

		// not found locally, so install it with dbc (which is downloaded if missing)
		if driverPath == "" {
			if driverName := conn.GetProp("driver_name"); driverName != "" && !cast.ToBool(os.Getenv("SLING_DISABLE_DBC_AUTO_INSTALL")) {
				if err := installDriverWithDbc(driverName); err != nil {
					g.Debug("could not auto-install ADBC driver %s: %s", driverName, err.Error())
				} else {
					driverPath = conn.resolveDriverPath()
				}
			}
		}

		if driverPath != "" {
			adbcProps["driver"] = driverPath
			g.Trace("auto-detected ADBC driver: %s", driverPath)
		}
	}

	// Set default entrypoint for known drivers if not explicitly provided
	if adbcProps["entrypoint"] == "" && adbcProps["driver_entrypoint"] == "" {
		driverName := conn.GetProp("driver_name")
		if ep := getDefaultEntrypoint(driverName); ep != "" {
			adbcProps["entrypoint"] = ep
		}
	}

	// Resolve the ADBC driver manager library path if not already set
	resolveDriverManagerLib()

	// not found on the system, so download it (conda-forge is the only channel
	// shipping prebuilt driver-manager binaries; dbc only provides drivers)
	if os.Getenv("ADBC_DRIVER_MANAGER_LIB") == "" && !cast.ToBool(os.Getenv("SLING_DISABLE_DBC_AUTO_INSTALL")) {
		if libPath, err := ensureDriverManagerLib(); err != nil {
			g.Debug("could not auto-download ADBC driver manager: %s", err.Error())
		} else {
			os.Setenv("ADBC_DRIVER_MANAGER_LIB", libPath)
			g.Trace("using downloaded ADBC driver manager: %s", libPath)
		}
	}

	db, err := drivermgr.Driver{}.NewDatabase(adbcProps)
	if err != nil {
		return g.Error(err, "could not init new ADBC database.%s See https://docs.slingdata.io/connections/database-connections/adbc",
			diagnoseADBCLoadError(err))
	}

	conn.db = db
	instance := Connection(conn)
	conn.BaseConn.instance = &instance

	// Determine driver type for template delegation
	driverName := conn.GetProp("driver_name")
	conn.driverType = GetArrowDBCDriverType(driverName)

	if err := conn.BaseConn.Init(); err != nil {
		return err
	}

	// Reload templates with driver-specific overrides
	// (BaseConn.Init() loaded the default ADBC template)
	return conn.LoadTemplates()
}

// isCxxABIError reports whether a load failure is due to the system libstdc++
// being older than the ADBC driver manager requires.
func isCxxABIError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "GLIBCXX_") || strings.Contains(msg, "CXXABI_")
}

// diagnoseADBCLoadError turns a cryptic dynamic-loader failure into actionable
// advice. The prebuilt ADBC libraries are built against newer toolchains than
// some supported distros ship, and the raw loader message ("version
// `GLIBCXX_3.4.29' not found") doesn't say what to do about it.
// Returns a leading-space message, or "" when nothing specific applies.
func diagnoseADBCLoadError(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()

	switch {
	case isCxxABIError(err):
		return g.F(" The ADBC driver manager needs a newer C++ runtime (libstdc++) than this system provides%s.%s",
			neededVersion(msg, "GLIBCXX_", "CXXABI_"), libStdCxxRemedy())

	case strings.Contains(msg, "GLIBC_"):
		return g.F(" The ADBC driver manager needs a newer C runtime (glibc) than this system provides%s."+
			" Upgrade the OS, or point ADBC_DRIVER_MANAGER_LIB at a build compatible with this system.",
			neededVersion(msg, "GLIBC_"))

	case strings.Contains(msg, "wrong ELF class"),
		strings.Contains(msg, "incompatible architecture"),
		strings.Contains(msg, "but wrong architecture"):
		return g.F(" The ADBC driver manager was built for a different CPU architecture than this machine (%s/%s)."+
			" Remove the cached copy under %s and retry, or set ADBC_DRIVER_MANAGER_LIB explicitly.",
			runtime.GOOS, runtime.GOARCH, filepath.Join(env.HomeBinDir(), "adbc"))

	case strings.Contains(msg, "ADBC_DRIVER_MANAGER_LIB"):
		// the fork already suggests the env var; don't repeat it
		return ""
	}

	return ""
}

// libStdCxxRemedy fetches a compatible libstdc++ and returns the exact command
// to use it. LD_PRELOAD is the only reliable fix: the loader resolves the
// manager's DT_NEEDED against whichever libstdc++.so.6 is already in the global
// scope, and by the time sling can dlopen anything the system copy is loaded —
// so the newer library must be in place before the process starts.
func libStdCxxRemedy() string {
	generic := " Install a newer libstdc++ (e.g. `apt install libstdc++6` on a current release," +
		" or `conda install -c conda-forge libstdcxx`), or point ADBC_DRIVER_MANAGER_LIB" +
		" at a build compatible with this system."

	if runtime.GOOS != "linux" {
		return generic
	}

	cacheDir := filepath.Join(env.HomeBinDir(), "adbc", AdbcDriverManagerVersion)
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return generic
	}
	libPath, err := ensureCompatibleLibStdCxx(cacheDir)
	if err != nil {
		g.Debug("could not fetch a compatible libstdc++: %s", err.Error())
		return generic
	}

	return g.F(" A compatible libstdc++ has been downloaded to %s —"+
		" re-run with it preloaded:\n\n    LD_PRELOAD=%s %s\n\n"+
		" To make this permanent, export LD_PRELOAD in your shell profile."+
		" Alternatively install a newer system libstdc++, or point"+
		" ADBC_DRIVER_MANAGER_LIB at a build compatible with this system.",
		libPath, libPath, strings.Join(os.Args, " "))
}

// neededVersion extracts the first required symbol version (e.g. GLIBCXX_3.4.29)
// mentioned in a loader error, formatted for inclusion in a sentence.
func neededVersion(msg string, prefixes ...string) string {
	for _, prefix := range prefixes {
		idx := strings.Index(msg, prefix)
		if idx < 0 {
			continue
		}
		rest := msg[idx:]
		end := strings.IndexFunc(rest, func(r rune) bool {
			return !(r >= '0' && r <= '9') && r != '.' && r != '_' &&
				!(r >= 'A' && r <= 'Z')
		})
		if end > 0 {
			return " (requires " + strings.TrimRight(rest[:end], "._") + ")"
		}
	}
	return ""
}

// getDefaultEntrypoint returns the ADBC driver init function name for known drivers.
// The ADBC driver manager uses this to locate the initialization symbol in the shared library.
func getDefaultEntrypoint(driverName string) string {
	mapping := map[string]string{
		"duckdb": "duckdb_adbc_init",
	}
	if ep, ok := mapping[strings.ToLower(driverName)]; ok {
		return ep
	}
	return ""
}

// resolveDriverManagerLib searches common installation paths for the ADBC driver manager
// shared library and sets the ADBC_DRIVER_MANAGER_LIB env var if found.
// This is called before loading the driver manager so it can be found without
// requiring the user to manually set the env var.
func resolveDriverManagerLib() {
	// Skip if already set
	if os.Getenv("ADBC_DRIVER_MANAGER_LIB") != "" {
		return
	}

	var libName string
	var searchPaths []string

	home, _ := os.UserHomeDir()

	switch runtime.GOOS {
	case "darwin":
		libName = "libadbc_driver_manager.dylib"
		searchPaths = []string{
			"/usr/local/lib",
			"/opt/homebrew/lib",
		}
		// Conda/mamba paths
		if home != "" {
			searchPaths = append(searchPaths,
				filepath.Join(home, "mambaforge", "lib"),
				filepath.Join(home, "miniforge3", "lib"),
				filepath.Join(home, "miniconda3", "lib"),
				filepath.Join(home, "anaconda3", "lib"),
			)
		}
		// Homebrew mambaforge cask path
		searchPaths = append(searchPaths,
			"/opt/homebrew/Caskroom/mambaforge/base/lib",
			"/opt/homebrew/Caskroom/miniforge/base/lib",
		)
		// pip install --user puts .dylib in site-packages
		if home != "" {
			pyGlob := filepath.Join(home, "Library", "Python", "3.*", "lib", "python", "site-packages", "adbc_driver_manager")
			if matches, _ := filepath.Glob(pyGlob); len(matches) > 0 {
				searchPaths = append(searchPaths, matches...)
			}
		}
	case "linux":
		libName = "libadbc_driver_manager.so"
		searchPaths = []string{
			"/usr/lib",
			"/usr/local/lib",
			"/usr/lib/x86_64-linux-gnu",
			"/usr/lib/aarch64-linux-gnu",
			"/lib",
			"/lib64",
		}
		if home != "" {
			searchPaths = append(searchPaths,
				filepath.Join(home, ".local", "lib"),
				filepath.Join(home, "mambaforge", "lib"),
				filepath.Join(home, "miniforge3", "lib"),
				filepath.Join(home, "miniconda3", "lib"),
			)
			// pip install --user puts .so in site-packages
			pyGlob := filepath.Join(home, ".local", "lib", "python3.*", "site-packages", "adbc_driver_manager")
			if matches, _ := filepath.Glob(pyGlob); len(matches) > 0 {
				searchPaths = append(searchPaths, matches...)
			}
		}
	case "windows":
		libName = "adbc_driver_manager.dll"
		// Conda puts DLLs in <prefix>\Library\bin, not <prefix>\lib.
		condaRoots := []string{}
		if home != "" {
			condaRoots = append(condaRoots,
				filepath.Join(home, "mambaforge"),
				filepath.Join(home, "miniforge3"),
				filepath.Join(home, "miniconda3"),
				filepath.Join(home, "anaconda3"),
			)
		}
		if localAppData := os.Getenv("LOCALAPPDATA"); localAppData != "" {
			condaRoots = append(condaRoots,
				filepath.Join(localAppData, "mambaforge"),
				filepath.Join(localAppData, "miniforge3"),
				filepath.Join(localAppData, "miniconda3"),
				filepath.Join(localAppData, "Continuum", "anaconda3"),
			)
		}
		if programData := os.Getenv("ProgramData"); programData != "" {
			condaRoots = append(condaRoots,
				filepath.Join(programData, "mambaforge"),
				filepath.Join(programData, "miniforge3"),
				filepath.Join(programData, "miniconda3"),
				filepath.Join(programData, "anaconda3"),
			)
		}
		for _, root := range condaRoots {
			searchPaths = append(searchPaths,
				filepath.Join(root, "Library", "bin"),
				// Active conda env rather than the base install
				filepath.Join(root, "envs", "*", "Library", "bin"),
			)
		}
		// An activated conda env exports its own prefix
		if prefix := os.Getenv("CONDA_PREFIX"); prefix != "" {
			searchPaths = append([]string{filepath.Join(prefix, "Library", "bin")}, searchPaths...)
		}
		// pip puts the DLL in site-packages
		if localAppData := os.Getenv("LOCALAPPDATA"); localAppData != "" {
			searchPaths = append(searchPaths,
				filepath.Join(localAppData, "Programs", "Python", "Python3*", "Lib", "site-packages", "adbc_driver_manager"),
			)
		}
		if home != "" {
			searchPaths = append(searchPaths,
				filepath.Join(home, "AppData", "Roaming", "Python", "Python3*", "site-packages", "adbc_driver_manager"),
			)
		}
		if programFiles := os.Getenv("ProgramFiles"); programFiles != "" {
			searchPaths = append(searchPaths, filepath.Join(programFiles, "ADBC", "bin"))
		}
	default:
		return
	}

	// Windows paths may contain globs (conda envs, versioned Python dirs)
	if runtime.GOOS == "windows" {
		expanded := make([]string, 0, len(searchPaths))
		for _, dir := range searchPaths {
			if strings.ContainsAny(dir, "*?") {
				matches, _ := filepath.Glob(dir)
				expanded = append(expanded, matches...)
				continue
			}
			expanded = append(expanded, dir)
		}
		searchPaths = expanded
	}

	for _, dir := range searchPaths {
		libPath := filepath.Join(dir, libName)
		if _, err := os.Stat(libPath); err == nil {
			os.Setenv("ADBC_DRIVER_MANAGER_LIB", libPath)
			g.Trace("auto-detected ADBC driver manager: %s", libPath)
			return
		}
	}
}

// GetArrowDBCDriverType maps ADBC driver names to corresponding database types
// This allows using driver-specific SQL templates
func GetArrowDBCDriverType(driverName string) dbio.Type {
	mapping := map[string]dbio.Type{
		"postgresql": dbio.TypeDbPostgres,
		"postgres":   dbio.TypeDbPostgres,
		"mssql":      dbio.TypeDbSQLServer,
		"sqlserver":  dbio.TypeDbSQLServer,
		"snowflake":  dbio.TypeDbSnowflake,
		"sqlite":     dbio.TypeDbSQLite,
		"duckdb":     dbio.TypeDbDuckDb,
		"bigquery":   dbio.TypeDbBigQuery,
		"mysql":      dbio.TypeDbMySQL,
		"trino":      dbio.TypeDbTrino,
	}
	if t, ok := mapping[strings.ToLower(driverName)]; ok {
		return t
	}
	return dbio.TypeDbArrowDBC // Fallback to ADBC template
}

// getArrowStringValue extracts a string value from an Arrow array at the given index.
// It handles String, LargeString, Binary, and LargeBinary types, and creates a copy
// of the string to avoid referencing Arrow buffer memory which may be freed.
func getArrowStringValue(arr arrow.Array, idx int) string {
	if arr.IsNull(idx) {
		return ""
	}
	switch a := arr.(type) {
	case *array.String:
		return strings.Clone(a.Value(idx))
	case *array.LargeString:
		return strings.Clone(a.Value(idx))
	case *array.Binary:
		return string(a.Value(idx))
	case *array.LargeBinary:
		return string(a.Value(idx))
	default:
		val := iop.GetValueFromArrowArray(arr, idx)
		if val != nil {
			return g.CastToString(val)
		}
		return ""
	}
}

// resolveDriverPath attempts to find the ADBC driver from various locations
// Search order:
// 1. Explicit 'driver' property
// 2. ADBC_DRIVER_PATH environment variable (colon-separated paths on Unix, semicolon on Windows)
// 3. dbc CLI installation paths (~/Library/Application Support/ADBC/Drivers, ~/.dbc/drivers, etc.)
// 4. System library paths (/usr/lib, /usr/local/lib, etc.)
func (conn *ArrowDBConn) resolveDriverPath() string {
	// Check if explicit 'driver' property is set
	if driver := conn.GetProp("driver"); driver != "" {
		return driver
	}

	// Get driver name hint from properties
	driverName := conn.GetProp("driver_name")
	if driverName == "" {
		return ""
	}

	home, err := os.UserHomeDir()
	if err != nil {
		home = ""
	}

	// Platform-specific extension and paths
	var ext string
	var driverPaths []string
	var pathSeparator string

	switch runtime.GOOS {
	case "darwin":
		ext = ".dylib"
		pathSeparator = ":"
		// macOS: dbc installs to ~/Library/Application Support/ADBC/Drivers
		if home != "" {
			driverPaths = []string{
				filepath.Join(home, "Library", "Application Support", "ADBC", "Drivers"),
				filepath.Join(home, ".dbc", "drivers"),
			}
		}
		// System paths for Homebrew and standard locations
		driverPaths = append(driverPaths,
			"/usr/local/lib",
			"/opt/homebrew/lib",
			"/usr/lib",
		)
	case "windows":
		ext = ".dll"
		pathSeparator = ";"
		// Windows: dbc installs to %APPDATA%\adbc\drivers
		appData := os.Getenv("APPDATA")
		if appData != "" {
			driverPaths = append(driverPaths, filepath.Join(appData, "adbc", "drivers"))
		}
		localAppData := os.Getenv("LOCALAPPDATA")
		if localAppData != "" {
			driverPaths = append(driverPaths, filepath.Join(localAppData, "ADBC", "Drivers"))
		}
		if home != "" {
			driverPaths = append(driverPaths,
				filepath.Join(home, ".config", "adbc", "drivers"),
				filepath.Join(home, ".dbc", "drivers"),
			)
		}
		// System paths
		programFiles := os.Getenv("ProgramFiles")
		if programFiles != "" {
			driverPaths = append(driverPaths, filepath.Join(programFiles, "ADBC", "lib"))
		}
	default:
		ext = ".so"
		pathSeparator = ":"
		// Linux: user paths first
		if home != "" {
			driverPaths = []string{
				filepath.Join(home, ".local", "share", "ADBC", "Drivers"),
				filepath.Join(home, ".config", "adbc", "drivers"),
				filepath.Join(home, ".dbc", "drivers"),
			}
		}
		// System paths where apt/deb packages install libraries
		driverPaths = append(driverPaths,
			"/usr/lib",
			"/usr/local/lib",
			"/usr/lib/x86_64-linux-gnu",  // Debian/Ubuntu amd64
			"/usr/lib/aarch64-linux-gnu", // Debian/Ubuntu arm64
			"/lib",
			"/lib64",
			"/opt/arrow/lib", // Common Arrow installation path
		)
	}

	// Check ADBC_DRIVER_PATH environment variable (takes priority after explicit driver property)
	if envPath := os.Getenv("ADBC_DRIVER_PATH"); envPath != "" {
		envPaths := strings.Split(envPath, pathSeparator)
		// Prepend env paths to search first (after explicit driver property)
		driverPaths = append(envPaths, driverPaths...)
	}

	// Look for driver file in each potential location
	for _, basePath := range driverPaths {
		// Try multiple patterns to find the driver
		patterns := []string{
			// Direct in path: /usr/lib/libadbc_driver_postgresql.so
			filepath.Join(basePath, "libadbc_driver_"+driverName+ext),
			filepath.Join(basePath, "*"+driverName+"*"+ext),
			// Pattern: basePath/postgresql/libadbc_driver_postgresql.so
			filepath.Join(basePath, driverName, "*"+driverName+"*"+ext),
			// Pattern: basePath/postgresql-1.9.0/libadbc_driver_postgresql.so
			filepath.Join(basePath, driverName+"-*", "*"+driverName+"*"+ext),
			// Pattern: basePath/*/libadbc_driver_postgresql.so
			filepath.Join(basePath, "*", "*"+driverName+"*"+ext),
		}

		for _, pattern := range patterns {
			matches, _ := filepath.Glob(pattern)
			if len(matches) > 0 {
				return matches[0]
			}
		}
	}

	return ""
}

// DbcVersion is the version of the dbc CLI to download when it's not installed
const DbcVersion = "0.3.0"

// AdbcDriverManagerVersion is the version of the ADBC driver manager to download.
// conda-forge is the only channel publishing prebuilt driver-manager shared
// libraries for every platform we support; dbc distributes drivers, not the manager.
const AdbcDriverManagerVersion = "1.12.0"

// condaDriverManagerBuilds maps GOOS/GOARCH to the conda-forge subdir and build
// string for libadbc-driver-manager. Build strings are version-specific, so these
// must be updated alongside AdbcDriverManagerVersion.
var condaDriverManagerBuilds = map[string]struct{ subdir, build string }{
	"linux/amd64":   {"linux-64", "hb700be7_0"},
	"linux/arm64":   {"linux-aarch64", "hfefdfc9_0"},
	"darwin/amd64":  {"osx-64", "h9536453_0"},
	"darwin/arm64":  {"osx-arm64", "hdf8b884_0"},
	"windows/amd64": {"win-64", "h49e36cd_0"},
}

// ensureDriverManagerLib downloads the ADBC driver manager shared library if it
// isn't already present, and returns its path. The manager is the library that
// sling's bindings dlopen; it then loads the individual database drivers.
func ensureDriverManagerLib() (libPath string, err error) {
	version := AdbcDriverManagerVersion
	if val := os.Getenv("ADBC_DRIVER_MANAGER_VERSION"); val != "" {
		version = val
	}

	var libName string
	switch runtime.GOOS {
	case "windows":
		libName = "adbc_driver_manager.dll"
	case "darwin":
		libName = "libadbc_driver_manager.dylib"
	default:
		libName = "libadbc_driver_manager.so"
	}

	folderPath := filepath.Join(env.HomeBinDir(), "adbc", version)
	libPath = filepath.Join(folderPath, libName)
	if g.PathExists(libPath) {
		return libPath, nil
	}

	build, ok := condaDriverManagerBuilds[runtime.GOOS+"/"+runtime.GOARCH]
	if !ok {
		return "", g.Error("no ADBC driver manager build for %s/%s", runtime.GOOS, runtime.GOARCH)
	}

	pkgURL := g.F("https://conda.anaconda.org/conda-forge/%s/libadbc-driver-manager-%s-%s.conda",
		build.subdir, version, build.build)

	pkgPath := filepath.Join(os.TempDir(), g.F("libadbc-driver-manager-%s.conda", version))
	defer os.Remove(pkgPath)

	g.Info("downloading ADBC driver manager %s for %s/%s", version, runtime.GOOS, runtime.GOARCH)
	if err = net.DownloadFile(pkgURL, pkgPath); err != nil {
		return "", g.Error(err, "unable to download ADBC driver manager")
	}

	if err = os.MkdirAll(folderPath, 0755); err != nil {
		return "", g.Error(err, "could not create adbc folder")
	}

	if err = extractCondaLib(pkgPath, folderPath, libName); err != nil {
		return "", g.Error(err, "could not extract ADBC driver manager")
	}

	if !g.PathExists(libPath) {
		return "", g.Error("cannot find %s after extracting driver manager", libPath)
	}

	return libPath, nil
}

// CondaLibStdCxxVersion is the conda-forge libstdcxx version providing a
// libstdc++ new enough for the ADBC driver manager on distros with an older
// system copy.
const CondaLibStdCxxVersion = "16.1.0"

// conda build hashes differ per architecture, so they must be listed
// explicitly. Update alongside CondaLibStdCxxVersion.
var condaLibStdCxxBuilds = map[string]struct{ subdir, build string }{
	"amd64": {"linux-64", "h934c35e_1"},
	"arm64": {"linux-aarch64", "hef695bb_1"},
}

// ensureCompatibleLibStdCxx downloads a libstdc++ new enough for the ADBC
// driver manager (it needs GLIBCXX_3.4.29; Ubuntu 20.04 ships 3.4.28) and
// returns its path, for the caller to suggest via LD_PRELOAD.
//
// It cannot be applied in-process: the loader resolves the manager's DT_NEEDED
// against whatever libstdc++.so.6 is already in the global scope, and the
// system copy is loaded before sling runs any code. Neither dlopen(RTLD_GLOBAL)
// nor os.Setenv("LD_LIBRARY_PATH") overrides an already-loaded soname.
//
// Called only after a C++ ABI load failure, so a system with a new enough
// libstdc++ never downloads anything.
func ensureCompatibleLibStdCxx(folderPath string) (libStdCxx string, err error) {
	libStdCxx = filepath.Join(folderPath, "libstdc++.so.6")

	if !g.PathExists(libStdCxx) {
		build, ok := condaLibStdCxxBuilds[runtime.GOARCH]
		if !ok {
			return "", g.Error("no libstdc++ build available for linux/%s", runtime.GOARCH)
		}

		pkgName := g.F("libstdcxx-%s-%s", CondaLibStdCxxVersion, build.build)
		pkgURL := g.F("https://conda.anaconda.org/conda-forge/%s/%s.conda", build.subdir, pkgName)

		pkgPath := filepath.Join(os.TempDir(), pkgName+".conda")
		defer os.Remove(pkgPath)

		g.Info("downloading a compatible libstdc++ for the ADBC driver manager")
		if err = net.DownloadFile(pkgURL, pkgPath); err != nil {
			return "", g.Error(err, "unable to download libstdc++")
		}

		if err = extractCondaLib(pkgPath, folderPath, "libstdc++.so.6"); err != nil {
			return "", g.Error(err, "could not extract libstdc++")
		}

		if !g.PathExists(libStdCxx) {
			return "", g.Error("libstdc++ not found after extraction")
		}
	}

	return libStdCxx, nil
}

// isSharedLibName reports whether a file name is a shared library, including
// versioned forms like libfoo.so.1.2.3 and libfoo.112.0.0.dylib.
func isSharedLibName(name string) bool {
	return strings.HasSuffix(name, ".dll") ||
		strings.HasSuffix(name, ".dylib") ||
		strings.Contains(name, ".so")
}

// libStem returns the part of a library file name before the extension, so
// versioned siblings can be matched: libstdc++.so.6 -> libstdc++, and
// libadbc_driver_manager.dylib -> libadbc_driver_manager.
func libStem(libName string) string {
	for _, ext := range []string{".so", ".dylib", ".dll"} {
		if i := strings.Index(libName, ext); i > 0 {
			return libName[:i]
		}
	}
	return libName
}

// extractCondaLib pulls libName out of a .conda package into destDir.
// A .conda file is a zip containing zstd-compressed tarballs; the payload we want
// is the "pkg-" entry. Libraries live under Library/bin on Windows and lib elsewhere.
func extractCondaLib(condaPath, destDir, libName string) (err error) {
	zr, err := zip.OpenReader(condaPath)
	if err != nil {
		return g.Error(err, "could not open conda package")
	}
	defer zr.Close()

	var pkgEntry *zip.File
	for _, f := range zr.File {
		if strings.HasPrefix(f.Name, "pkg-") && strings.HasSuffix(f.Name, ".tar.zst") {
			pkgEntry = f
			break
		}
	}
	if pkgEntry == nil {
		return g.Error("no pkg payload found in conda package")
	}

	rc, err := pkgEntry.Open()
	if err != nil {
		return g.Error(err, "could not open conda payload")
	}
	defer rc.Close()

	zstdReader, err := zstd.NewReader(rc)
	if err != nil {
		return g.Error(err, "could not create zstd reader")
	}
	defer zstdReader.Close()

	// resolved lazily: the versioned file is the real library, the plain name a symlink to it
	symlinks := map[string]string{}
	extracted := map[string]bool{}

	tr := tar.NewReader(zstdReader)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return g.Error(err, "could not read conda tar entry")
		}

		// only the shared library itself (and its versioned siblings), not headers.
		// libName is e.g. libadbc_driver_manager.so or libstdc++.so.6; matching on
		// the stem catches libfoo.so.1.2.3 and libfoo.112.0.0.dylib alike.
		base := filepath.Base(header.Name)
		if !strings.HasPrefix(base, libStem(libName)) || !isSharedLibName(base) {
			continue
		}

		switch header.Typeflag {
		case tar.TypeSymlink:
			symlinks[base] = filepath.Base(header.Linkname)
		case tar.TypeReg:
			target := filepath.Join(destDir, base)
			out, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0755)
			if err != nil {
				return g.Error(err, "could not create %s", target)
			}
			if _, err = io.Copy(out, tr); err != nil {
				out.Close()
				return g.Error(err, "could not write %s", target)
			}
			out.Close()
			extracted[base] = true
		}
	}

	// the plain library name is a symlink in the package; copy the real file into place
	if !extracted[libName] {
		if link, ok := symlinks[libName]; ok && extracted[link] {
			data, err := os.ReadFile(filepath.Join(destDir, link))
			if err != nil {
				return g.Error(err, "could not read %s", link)
			}
			if err = os.WriteFile(filepath.Join(destDir, libName), data, 0755); err != nil {
				return g.Error(err, "could not write %s", libName)
			}
		}
	}

	return nil
}

// EnsureBinDbc returns the path to the dbc CLI, downloading it if missing.
// dbc (https://columnar.tech/dbc) installs and manages ADBC drivers.
func EnsureBinDbc() (binPath string, err error) {
	version := DbcVersion
	if val := os.Getenv("DBC_VERSION"); val != "" {
		version = val
	}

	// use specified path to dbc binary
	if envPath := os.Getenv("DBC_PATH"); envPath != "" {
		if !g.PathExists(envPath) {
			return "", g.Error("dbc binary not found: %s", envPath)
		}
		if stat, _ := os.Stat(envPath); stat.IsDir() {
			return "", g.Error("DBC_PATH provided is a directory, should be a file: %s", envPath)
		}
		return envPath, nil
	}

	extension := lo.Ternary(runtime.GOOS == "windows", ".exe", "")

	// an existing dbc on PATH is preferred over downloading our own
	if p, err := exec.LookPath("dbc" + extension); err == nil {
		return p, nil
	}

	folderPath := filepath.Join(env.HomeBinDir(), "dbc", version)
	binPath = filepath.Join(folderPath, "dbc"+extension)
	if g.PathExists(binPath) {
		return binPath, nil
	}

	// archives are flat, with the binary at the root
	const baseURL = "https://github.com/columnar-tech/dbc/releases/download/v{version}/dbc-{os}-{arch}-{version}.{ext}"

	var arch, archiveExt string
	switch runtime.GOARCH {
	case "amd64":
		arch = "amd64"
	case "arm64":
		arch = "arm64"
	default:
		return "", g.Error("dbc is not available for %s/%s", runtime.GOOS, runtime.GOARCH)
	}

	switch runtime.GOOS {
	case "windows":
		archiveExt = "zip"
		if arch != "amd64" {
			// no windows/arm64 build; the amd64 binary runs under emulation
			arch = "amd64"
		}
	case "darwin", "linux":
		archiveExt = "tar.gz"
	default:
		return "", g.Error("dbc is not available for %s/%s", runtime.GOOS, runtime.GOARCH)
	}

	downloadURL := g.R(baseURL,
		"version", version, "os", runtime.GOOS, "arch", arch, "ext", archiveExt)

	archivePath := filepath.Join(os.TempDir(), g.F("dbc-%s.%s", version, archiveExt))
	defer os.Remove(archivePath)

	g.Info("downloading dbc %s for %s/%s", version, runtime.GOOS, arch)
	if err = net.DownloadFile(downloadURL, archivePath); err != nil {
		return "", g.Error(err, "unable to download dbc binary")
	}

	if err = os.MkdirAll(folderPath, 0755); err != nil {
		return "", g.Error(err, "could not create dbc folder")
	}

	if archiveExt == "zip" {
		if _, err = iop.Unzip(archivePath, folderPath); err != nil {
			return "", g.Error(err, "error unzipping dbc archive")
		}
	} else if err = g.ExtractTarGz(archivePath, folderPath); err != nil {
		return "", g.Error(err, "error extracting dbc archive")
	}

	if !g.PathExists(binPath) {
		return "", g.Error("cannot find dbc binary at %s after extraction", binPath)
	}

	if err = os.Chmod(binPath, 0755); err != nil {
		return "", g.Error(err, "could not make dbc executable")
	}

	return binPath, nil
}

// installDriverWithDbc installs an ADBC driver via the dbc CLI, downloading dbc if needed.
func installDriverWithDbc(driverName string) (err error) {
	dbcPath, err := EnsureBinDbc()
	if err != nil {
		return g.Error(err, "could not obtain dbc CLI")
	}

	g.Info("installing ADBC driver %s via dbc", driverName)
	out, err := exec.Command(dbcPath, "install", "--level", "user", driverName).CombinedOutput()
	if err != nil {
		return g.Error(err, "could not install ADBC driver %s: %s", driverName, string(out))
	}

	g.Debug("dbc install %s: %s", driverName, strings.TrimSpace(string(out)))
	return nil
}

// Connect opens the ADBC connection
func (conn *ArrowDBConn) Connect(timeOut ...int) (err error) {
	// Re-initialize database if it was closed
	if conn.db == nil {
		if err := conn.Init(); err != nil {
			return g.Error(err, "could not re-initialize ADBC database")
		}
	}

	conn.Conn, err = conn.db.Open(conn.context.Ctx)
	if err != nil {
		return g.Error(err, "could not connect to ADBC database")
	}

	if !cast.ToBool(conn.GetProp("silent")) {
		g.Debug(`opened "%s" connection (%s)`, conn.Type, conn.GetProp("sling_conn_id"))
	}

	conn.SetProp("connected", "true")
	conn.postConnect()

	return nil
}

// Close closes the ADBC connection and database
func (conn *ArrowDBConn) Close() error {
	var connErr, dbErr error

	if conn.Conn != nil {
		connErr = conn.Conn.Close()
		conn.Conn = nil
	}

	if conn.db != nil {
		dbErr = conn.db.Close()
		conn.db = nil
	}

	if !cast.ToBool(conn.GetProp("silent")) && cast.ToBool(conn.GetProp("connected")) {
		g.Debug(`closed "%s" connection (%s)`, conn.Type, conn.GetProp("sling_conn_id"))
	}

	conn.SetProp("connected", "false")

	if connErr != nil {
		return g.Error(connErr, "error closing ADBC connection")
	}
	if dbErr != nil {
		return g.Error(dbErr, "error closing ADBC database")
	}

	return nil
}

// GetTemplateValue returns the template value for the given path
// It first checks the driver-specific template, then falls back to ADBC template
func (conn *ArrowDBConn) GetTemplateValue(path string) string {
	// First try driver-specific template
	if conn.driverType != "" && conn.driverType != dbio.TypeDbArrowDBC {
		value := conn.driverType.GetTemplateValue(path)
		if value != "" {
			return value
		}
	}
	// Fall back to ADBC template
	return conn.Type.GetTemplateValue(path)
}

// GetNativeType returns the native column type from generic
func (conn *ArrowDBConn) GetNativeType(col iop.Column) (nativeType string, err error) {
	var ct iop.ColumnTyping
	if val := conn.GetProp("column_typing"); val != "" {
		g.Unmarshal(val, &ct)
	}
	return col.GetNativeType(conn.driverType, ct)
}

func (conn *ArrowDBConn) Template() dbio.Template {
	return conn.template
}

func (conn *ArrowDBConn) Quote(field string) string {
	return conn.template.Quote(field)
}

func (conn *ArrowDBConn) Unquote(field string) string {
	return conn.template.Unquote(field)
}

// LoadTemplates loads the appropriate yaml template
// For ADBC, it merges the driver-specific template with the ADBC template
// Driver template is base, ADBC template overrides for ADBC-specific behavior
func (conn *ArrowDBConn) LoadTemplates() error {
	// Load ADBC template without base
	adbcTemplate, err := conn.Type.Template(false)
	if err != nil {
		return g.Error(err, "could not load ADBC template")
	}

	// If we have a driver type, start with driver template as base
	if conn.driverType != "" && conn.driverType != dbio.TypeDbArrowDBC {
		driverTemplate, err := conn.driverType.Template()
		if err != nil {
			g.Warn("could not load driver template for %s: %v", conn.driverType, err)
			conn.template = adbcTemplate
			return nil
		}

		// Start with driver template, then overlay ADBC-specific values
		// This allows driver SQL syntax to be used, with ADBC overrides where needed
		for k, v := range adbcTemplate.Core {
			driverTemplate.Core[k] = v
		}
		for k, v := range adbcTemplate.Metadata {
			driverTemplate.Metadata[k] = v
		}
		for k, v := range adbcTemplate.Analysis {
			driverTemplate.Analysis[k] = v
		}
		for k, v := range adbcTemplate.Function {
			driverTemplate.Function[k] = v
		}
		for k, v := range adbcTemplate.Variable {
			driverTemplate.Variable[k] = v
		}

		conn.template = driverTemplate
		conn.Type = conn.driverType

		return nil
	}

	// load with base
	conn.template, err = conn.Type.Template(true)
	if err != nil {
		return g.Error(err, "could not load ADBC template")
	}

	return nil
}

// adbcResult implements sql.Result for ADBC operations
type adbcResult struct {
	rowsAffected int64
}

func (r adbcResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (r adbcResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// ExecContext executes a SQL statement (read-only operations)
func (conn *ArrowDBConn) ExecContext(ctx context.Context, sql string, args ...interface{}) (result sql.Result, err error) {
	if conn.Conn == nil {
		return nil, g.Error("ADBC connection is not open")
	}

	stmt, err := conn.Conn.NewStatement()
	if err != nil {
		return nil, g.Error(err, "could not create ADBC statement")
	}
	defer stmt.Close()

	// Handle argument substitution if any
	if len(args) > 0 {
		for _, arg := range args {
			switch val := arg.(type) {
			case int, int64, int8, int32, int16:
				sql = strings.Replace(sql, "?", cast.ToString(val), 1)
			case float32, float64:
				sql = strings.Replace(sql, "?", cast.ToString(val), 1)
			case nil:
				sql = strings.Replace(sql, "?", "NULL", 1)
			default:
				v := strings.ReplaceAll(cast.ToString(val), "'", "''")
				sql = strings.Replace(sql, "?", "'"+v+"'", 1)
			}
		}
	}

	conn.LogSQL(sql)

	if err := stmt.SetSqlQuery(sql); err != nil {
		return nil, g.Error(err, "could not set SQL query")
	}

	rowsAffected, err := stmt.ExecuteUpdate(ctx)
	if err != nil {
		return nil, g.Error(err, "could not execute SQL")
	}

	return adbcResult{rowsAffected: rowsAffected}, nil
}

// StreamRowsContext streams query results as a datastream using Arrow record batches
func (conn *ArrowDBConn) StreamRowsContext(ctx context.Context, sql string, options ...map[string]interface{}) (ds *iop.Datastream, err error) {
	if conn.Conn == nil {
		return nil, g.Error("ADBC connection is not open")
	}

	queryContext := g.NewContext(ctx)

	// Get options
	limit := uint64(0)
	if len(options) > 0 {
		if val, ok := options[0]["limit"]; ok {
			limit = cast.ToUint64(val)
		}
	}

	// Create and configure statement
	stmt, err := conn.Conn.NewStatement()
	if err != nil {
		return nil, g.Error(err, "could not create ADBC statement")
	}

	conn.LogSQL(sql)

	if err := stmt.SetSqlQuery(sql); err != nil {
		stmt.Close()
		return nil, g.Error(err, "could not set SQL query")
	}

	// Execute query
	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		stmt.Close()
		return nil, g.Error(err, "could not execute query")
	}

	// Convert Arrow schema to columns
	schema := reader.Schema()
	columns := iop.ArrowSchemaToColumns(schema)

	// Create the next function for streaming records
	makeNextFunc := func() func(it *iop.Iterator) bool {
		var currentRecord arrow.Record
		var previousRecord arrow.Record // Keep previous record until next iteration to prevent string memory corruption
		var currentRowIdx int
		var recordChan = make(chan arrow.Record, 10)

		// Stream records in a goroutine
		go func() {
			defer close(recordChan)
			defer reader.Release()
			defer stmt.Close()

			for reader.Next() {
				record := reader.Record()
				record.Retain() // Retain so it doesn't get freed
				select {
				case recordChan <- record:
				case <-queryContext.Ctx.Done():
					record.Release()
					return
				}
			}

			if err := reader.Err(); err != nil {
				queryContext.CaptureErr(g.Error(err, "error reading Arrow records"))
			}
		}()

		return func(it *iop.Iterator) bool {
			if limit > 0 && uint64(it.Counter) >= limit {
				return false
			}

			// Release the previous record (now safe since its data has been consumed)
			if previousRecord != nil {
				previousRecord.Release()
				previousRecord = nil
			}

			// Check if we need to fetch next record batch
			if currentRecord == nil || currentRowIdx >= int(currentRecord.NumRows()) {
				// Move current to previous (will be released on next iteration)
				previousRecord = currentRecord
				currentRecord = nil

				select {
				case record, ok := <-recordChan:
					if !ok {
						// Channel closed, no more records
						return false
					}
					currentRecord = record
					currentRowIdx = 0
				case <-queryContext.Ctx.Done():
					return false
				}
			}

			// Convert current row to interface{} slice
			// Copy string values since Arrow buffer memory may be reused
			it.Row = make([]interface{}, currentRecord.NumCols())
			for colIdx := 0; colIdx < int(currentRecord.NumCols()); colIdx++ {
				col := currentRecord.Column(colIdx)
				val := iop.GetValueFromArrowArray(col, currentRowIdx)
				// Copy string values to avoid referencing Arrow buffer memory
				if s, ok := val.(string); ok {
					val = strings.Clone(s)
				}
				it.Row[colIdx] = val
			}

			currentRowIdx++
			return true
		}
	}

	ds = iop.NewDatastreamIt(queryContext.Ctx, columns, makeNextFunc())
	ds.NoDebug = strings.Contains(sql, noDebugKey)
	ds.Inferred = !InferDBStream && ds.Columns.Sourced()

	if !ds.NoDebug {
		ds.SetMetadata(conn.GetProp("METADATA"))
		ds.SetConfig(conn.Props())
	}

	err = ds.Start()
	if err != nil {
		queryContext.Cancel()
		return ds, g.Error(err, "could not start datastream")
	}

	return ds, nil
}

// GetSQLColumns returns columns for a SQL query using Arrow schema
// This avoids wrapping with LIMIT which may not work for all database types
func (conn *ArrowDBConn) GetSQLColumns(table Table) (columns iop.Columns, err error) {
	if !table.IsQuery() {
		return conn.GetColumns(table.FullName())
	}

	// For ADBC, we can execute the query directly and get schema from Arrow
	// Use limit 0 approach by wrapping, but if that fails, execute directly
	sql := table.SQL
	if sql == "" {
		sql = table.Select()
	}

	// Execute and get columns from Arrow schema directly
	ds, err := conn.StreamRowsContext(conn.Context().Ctx, sql, g.M("limit", 1))
	if err != nil {
		return columns, g.Error(err, "GetSQLColumns Error")
	}

	err = ds.WaitReady()
	if err != nil {
		return columns, g.Error(err, "Datastream Error")
	}

	ds.Collect(0) // advance the datastream so it can close
	return ds.Columns, nil
}

// BulkExportStream streams the rows in bulk
func (conn *ArrowDBConn) BulkExportStream(table Table) (ds *iop.Datastream, err error) {
	return conn.StreamRowsContext(conn.Context().Ctx, table.Select())
}

// BulkExportFlow exports data as a dataflow
func (conn *ArrowDBConn) BulkExportFlow(table Table) (df *iop.Dataflow, err error) {
	// Build the query
	sql := table.Select()
	if table.SQL != "" {
		sql = table.SQL
	}

	ds, err := conn.StreamRowsContext(conn.Context().Ctx, sql)
	if err != nil {
		return nil, g.Error(err, "could not stream rows")
	}

	df, err = iop.MakeDataFlow(ds)
	if err != nil {
		return nil, g.Error(err, "could not create dataflow")
	}

	return df, nil
}

// BulkImportFlow imports data from a dataflow using ADBC bulk ingestion
func (conn *ArrowDBConn) BulkImportFlow(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	defer df.CleanUp()

	if conn.Conn == nil {
		return 0, g.Error("ADBC connection is not open")
	}

	for ds := range df.StreamCh {
		if err = ds.WaitReady(); err != nil {
			return count, g.Error(err, "error waiting for datastream")
		}

		cnt, err := conn.BulkImportStream(tableFName, ds)
		if err != nil {
			return count, g.Error(err, "error importing stream")
		}
		count += cnt
	}

	if err = df.Err(); err != nil {
		return count, g.Error(err, "error in dataflow")
	}

	return count, nil
}

// BulkImportStream imports data from a datastream using ADBC bulk ingestion
func (conn *ArrowDBConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	if conn.Conn == nil {
		return 0, g.Error("ADBC connection is not open")
	}

	// Parse table name to get catalog and schema
	table, err := ParseTableName(tableFName, conn.Type)
	if err != nil {
		return count, g.Error(err, "could not parse table name: %s", tableFName)
	}

	// Get ingest mode from property, default to append
	ingestMode := conn.getIngestMode()

	// Target the catalog/schema of the table, not the connection defaults
	opts := adbc.IngestStreamOptions{
		Catalog:  table.Database,
		DBSchema: table.Schema,
	}

	// For 2-part targets (schema.table), ParseTableName leaves table.Database empty
	if opts.Catalog == "" {
		opts.Catalog = conn.GetProp("database")
	}

	g.Trace("arrow schema => %s", iop.ColumnsToArrowSchema(ds.Columns))

	for batch := range ds.BatchChan {
		// Convert batch to Arrow record reader
		reader, err := conn.batchToRecordReader(batch)
		if err != nil {
			return count, g.Error(err, "error converting batch to Arrow")
		}

		ingested, err := adbc.IngestStream(
			conn.Context().Ctx,
			conn.Conn,
			reader,
			table.Name,
			ingestMode,
			opts,
		)
		reader.Release()

		if err != nil {
			return count, g.Error(err, "error ingesting batch via ADBC")
		}

		count += uint64(ingested)
	}

	return count, nil
}

// getIngestMode returns the ADBC ingest mode based on the ingest_mode property
// Valid values: create, append, replace, create_append
// Default: append
func (conn *ArrowDBConn) getIngestMode() string {
	mode := strings.ToLower(conn.GetProp("ingest_mode"))
	switch mode {
	case "create":
		return adbc.OptionValueIngestModeCreate
	case "replace":
		return adbc.OptionValueIngestModeReplace
	case "create_append":
		return adbc.OptionValueIngestModeCreateAppend
	case "append", "":
		return adbc.OptionValueIngestModeAppend
	default:
		g.Warn("Unknown ingest_mode '%s', using 'append'", mode)
		return adbc.OptionValueIngestModeAppend
	}
}

// batchToRecordReader converts an iop.Batch to an Arrow RecordReader
// It consumes all rows from the batch channel
func (conn *ArrowDBConn) batchToRecordReader(batch *iop.Batch) (array.RecordReader, error) {
	// Create Arrow schema from columns
	schema := iop.ColumnsToArrowSchema(batch.Columns)

	// Create memory allocator
	mem := memory.NewGoAllocator()

	// Create record builder
	builder := array.NewRecordBuilder(mem, schema)

	// Consume all rows from the batch channel and append to builder
	rowCount := 0
	for row := range batch.Rows {
		for colIdx, col := range batch.Columns {
			var val interface{}
			if colIdx < len(row) {
				val = row[colIdx]
			}
			iop.AppendToBuilder(builder.Field(colIdx), &col, val)
		}
		rowCount++
	}

	// Build the record
	record := builder.NewRecord()
	builder.Release()

	if rowCount == 0 {
		// Return empty reader with schema
		record.Release()
		return array.NewRecordReader(schema, []arrow.Record{})
	}

	// Create a RecordReader from the single record
	reader, err := array.NewRecordReader(schema, []arrow.Record{record})
	if err != nil {
		record.Release()
		return nil, g.Error(err, "error creating record reader")
	}

	// Note: record will be released when reader is released
	return reader, nil
}

// NewAdbcConn creates a new ADBC conn from a parent conn
// constructs the connection string with complete URIs/paths for each database type
func NewAdbcConn(parentConn Connection) (adbcConn Connection, err error) {
	connMap := map[string]string{
		"url":  "adbc://",
		"name": parentConn.GetProp("name") + "-adbc",
	}

	// Get connection info and property accessor
	info := parentConn.Info()
	getProp := func(key string) string {
		return parentConn.GetProp(key)
	}

	// Copy ADBC-specific properties from parent (adbc.*, driver_name, driver)
	copyAdbcProperties(parentConn, connMap)

	switch parentConn.GetType() {
	case dbio.TypeDbPostgres:
		connMap["driver_name"] = "postgresql"
		connMap["uri"] = buildPostgresAdbcURI(info, getProp)

	case dbio.TypeDbSQLServer:
		connMap["driver_name"] = "mssql"
		connMap["uri"] = buildSQLServerAdbcURI(info, getProp)

	case dbio.TypeDbSnowflake:
		connMap["driver_name"] = "snowflake"
		// the driver has no "adbc.snowflake.sql.uri" option; the generic "uri"
		// is parsed with gosnowflake.ParseDSN
		connMap["uri"] = buildSnowflakeAdbcURI(info, getProp)

	case dbio.TypeDbSQLite:
		connMap["driver_name"] = "sqlite"
		connMap["uri"] = buildSQLiteAdbcURI(info, getProp)

	case dbio.TypeDbDuckDb:
		connMap["driver_name"] = "duckdb"
		connMap["path"] = buildDuckDbAdbcPath(info, getProp)

	case dbio.TypeDbBigQuery:
		connMap["driver_name"] = "bigquery"
		buildBigQueryAdbcConfig(getProp, connMap)

	case dbio.TypeDbMySQL:
		connMap["driver_name"] = "mysql"
		connMap["uri"] = buildMySQLAdbcURI(info, getProp)

	case dbio.TypeDbTrino:
		connMap["driver_name"] = "trino"
		connMap["uri"] = parentConn.GetProp("http_url")
		// Flight SQL auth via separate options
		if info.User != "" {
			connMap["username"] = info.User
		}
		if info.Password != "" {
			connMap["password"] = info.Password
		}
	}

	if uri := parentConn.GetProp("adbc_uri"); uri != "" {
		switch parentConn.GetType() {
		case dbio.TypeDbDuckDb:
			connMap["path"] = uri
		case dbio.TypeDbBigQuery:
			// no uri option exists, and unknown keys are rejected
			g.Warn("adbc_uri is not supported for BigQuery, ignoring")
		default:
			connMap["uri"] = uri
		}
	}

	props := g.MapToKVArr(connMap)
	c, err := NewConnContext(parentConn.Context().Ctx, "adbc://", props...)
	if err != nil {
		return nil, g.Error(err, "could not init ADBC Connection")
	}

	return c, c.Init()
}

// copyAdbcProperties copies ADBC-specific and driver properties from parent to connMap
func copyAdbcProperties(parentConn Connection, connMap map[string]string) {
	for key, val := range parentConn.Props() {
		// Pass through adbc.* properties (driver-specific options)
		if strings.HasPrefix(key, "adbc.") {
			connMap[key] = val
		}
	}
	// Pass driver_name for driver resolution in Init()
	if dn := parentConn.GetProp("driver_name"); dn != "" {
		connMap["driver_name"] = dn
	}
	// Pass driver path if explicitly set
	if driver := parentConn.GetProp("driver"); driver != "" {
		connMap["driver"] = driver
	}
}

// buildPostgresAdbcURI builds a PostgreSQL ADBC connection URI
// Format: postgresql://[user[:password]@][host][:port][/dbname][?params]
func buildPostgresAdbcURI(info ConnInfo, getProp func(string) string) string {
	var uri strings.Builder
	uri.WriteString("postgresql://")

	// User and password
	if info.User != "" {
		uri.WriteString(url.QueryEscape(info.User))
		if info.Password != "" {
			uri.WriteString(":")
			uri.WriteString(url.QueryEscape(info.Password))
		}
		uri.WriteString("@")
	}

	// Host and port
	if info.Host != "" {
		uri.WriteString(info.Host)
		if info.Port > 0 {
			uri.WriteString(fmt.Sprintf(":%d", info.Port))
		}
	}

	// Database
	if info.Database != "" {
		uri.WriteString("/")
		uri.WriteString(info.Database)
	}

	// Query parameters
	params := url.Values{}
	if val := getProp("sslmode"); val != "" {
		params.Set("sslmode", val)
	}
	if val := getProp("connect_timeout"); val != "" {
		params.Set("connect_timeout", val)
	}
	if val := getProp("application_name"); val != "" {
		params.Set("application_name", val)
	}

	if len(params) > 0 {
		uri.WriteString("?")
		uri.WriteString(params.Encode())
	}

	result := uri.String()
	return result
}

// buildSnowflakeAdbcURI builds a Snowflake ADBC connection URI
// Format: user[:password]@account/database/schema[?params]
func buildSnowflakeAdbcURI(info ConnInfo, getProp func(string) string) string {
	var uri strings.Builder

	// User and secret. A programmatic access token is carried in the password
	// position, which is where gosnowflake expects it.
	authenticator := getProp("authenticator")
	secret := info.Password
	if strings.EqualFold(authenticator, "programmatic_access_token") {
		if token := getProp("token"); token != "" {
			secret = token
		}
	}

	if info.User != "" {
		uri.WriteString(url.QueryEscape(info.User))
		if secret != "" {
			uri.WriteString(":")
			uri.WriteString(url.QueryEscape(secret))
		}
		uri.WriteString("@")
	}

	// Account - prefer explicit "account" property, then extract from host
	account := getProp("account")
	if account == "" {
		// Handle full host: "account.region.snowflakecomputing.com" → "account.region"
		account = strings.TrimSuffix(info.Host, ".snowflakecomputing.com")
	}
	uri.WriteString(account)

	// Database and schema
	if info.Database != "" {
		uri.WriteString("/")
		uri.WriteString(info.Database)
		if info.Schema != "" {
			uri.WriteString("/")
			uri.WriteString(info.Schema)
		}
	}

	// Query parameters
	params := url.Values{}
	if info.Warehouse != "" {
		params.Set("warehouse", info.Warehouse)
	}
	if info.Role != "" {
		params.Set("role", info.Role)
	}
	if authenticator != "" {
		params.Set("authenticator", authenticator)
	}
	// key-pair auth: gosnowflake reads the DER key from the DSN
	if epk := getProp("encoded_private_key"); epk != "" {
		params.Set("authenticator", "SNOWFLAKE_JWT")
		params.Set("privateKey", epk)
	}

	if len(params) > 0 {
		uri.WriteString("?")
		uri.WriteString(params.Encode())
	}

	result := uri.String()
	return result
}

// buildSQLiteAdbcURI builds a SQLite ADBC connection URI
// Format: file:path/to/file.db or :memory:
func buildSQLiteAdbcURI(info ConnInfo, getProp func(string) string) string {
	// Get database path
	dbPath := info.Database
	if dbPath == "" {
		dbPath = getProp("database")
	}
	if dbPath == "" {
		dbPath = ":memory:"
	}

	// If it's a file path and doesn't start with "file:" or ":memory:", add "file:" prefix
	if dbPath != ":memory:" && !strings.HasPrefix(dbPath, "file:") {
		dbPath = "file:" + dbPath
	}

	return dbPath
}

// buildDuckDbAdbcPath builds a DuckDB ADBC path parameter
// DuckDB uses 'path' parameter instead of 'uri'
// Format: /path/to/file.db or :memory:
func buildDuckDbAdbcPath(info ConnInfo, getProp func(string) string) string {
	// Get database path
	dbPath := info.Database
	if dbPath == "" {
		dbPath = getProp("database")
	}
	if dbPath == "" {
		dbPath = ":memory:"
	}

	g.Debug("Built DuckDB ADBC path: %s", dbPath)
	return dbPath
}

// BigQuery ADBC option keys and auth_type values. The driver rejects unknown
// options outright, and auth_type values are fully qualified, not bare words.
const (
	bqOptProjectID       = "adbc.bigquery.sql.project_id"
	bqOptDatasetID       = "adbc.bigquery.sql.dataset_id"
	bqOptLocation        = "adbc.bigquery.sql.location"
	bqOptAuthType        = "adbc.bigquery.sql.auth_type"
	bqOptAuthCredentials = "adbc.bigquery.sql.auth_credentials"

	bqAuthJSONFile   = "adbc.bigquery.sql.auth_type.json_credential_file"
	bqAuthJSONString = "adbc.bigquery.sql.auth_type.json_credential_string"
	bqAuthDefault    = "adbc.bigquery.sql.auth_type.app_default_credentials"
)

// buildBigQueryAdbcConfig populates ADBC BigQuery configuration parameters
// BigQuery uses configuration parameters instead of URI format
func buildBigQueryAdbcConfig(getProp func(string) string, connMap map[string]string) {
	// Required: Project ID
	if projectID := getProp("project"); projectID != "" {
		connMap[bqOptProjectID] = projectID
	} else if projectID := getProp("project_id"); projectID != "" {
		connMap[bqOptProjectID] = projectID
	}

	// Auth type and credentials travel together: auth_type says how to read
	// the single auth_credentials value.
	keyBody, keyFile := getProp("GC_KEY_BODY"), getProp("GC_KEY_FILE")
	authType := getProp("auth_type")
	switch {
	case authType != "":
		// allow a bare value to be passed through in qualified form
		if !strings.HasPrefix(authType, "adbc.bigquery.sql.auth_type.") {
			authType = "adbc.bigquery.sql.auth_type." + authType
		}
		if keyBody != "" {
			connMap[bqOptAuthCredentials] = keyBody
		} else if keyFile != "" {
			connMap[bqOptAuthCredentials] = keyFile
		}
	case keyBody != "":
		authType = bqAuthJSONString
		connMap[bqOptAuthCredentials] = keyBody
	case keyFile != "":
		authType = bqAuthJSONFile
		connMap[bqOptAuthCredentials] = keyFile
	default:
		authType = bqAuthDefault
	}
	connMap[bqOptAuthType] = authType

	// Optional: Dataset/Schema
	if dataset := getProp("dataset"); dataset != "" {
		connMap[bqOptDatasetID] = dataset
	} else if schema := getProp("schema"); schema != "" {
		connMap[bqOptDatasetID] = schema
	}

	// Optional: Location/Region
	if location := getProp("location"); location != "" {
		connMap[bqOptLocation] = location
	}

	g.Debug("Built BigQuery ADBC configuration with auth_type=%s", authType)
}

// buildSQLServerAdbcURI builds a SQL Server ADBC connection URI
// Format: Server=host,port;Database=db;User Id=user;Password=pwd;
func buildSQLServerAdbcURI(info ConnInfo, getProp func(string) string) string {
	var parts []string

	// Server
	if info.Host != "" {
		serverStr := info.Host
		if info.Port > 0 {
			serverStr = fmt.Sprintf("%s,%d", info.Host, info.Port)
		}
		parts = append(parts, fmt.Sprintf("Server=%s", serverStr))
	}

	// Database
	if info.Database != "" {
		parts = append(parts, fmt.Sprintf("Database=%s", info.Database))
	}

	// User and Password
	if info.User != "" {
		parts = append(parts, fmt.Sprintf("User Id=%s", info.User))
	}
	if info.Password != "" {
		// ODBC escaping: wrap in braces if contains special chars, double any }
		pwd := info.Password
		if strings.ContainsAny(pwd, ";{}") {
			pwd = "{" + strings.ReplaceAll(pwd, "}", "}}") + "}"
		}
		parts = append(parts, fmt.Sprintf("Password=%s", pwd))
	}

	// Additional parameters
	if encrypt := getProp("encrypt"); encrypt != "" {
		parts = append(parts, fmt.Sprintf("Encrypt=%s", encrypt))
	}
	if trustCert := getProp("TrustServerCertificate"); trustCert != "" {
		parts = append(parts, fmt.Sprintf("TrustServerCertificate=%s", trustCert))
	}

	result := strings.Join(parts, ";")
	return result
}

// buildMySQLAdbcURI builds a MySQL ADBC connection URI
// Note: MySQL does not have an official ADBC driver
// Format: user:password@tcp(host:port)/database
func buildMySQLAdbcURI(info ConnInfo, getProp func(string) string) string {
	var uri strings.Builder

	// User and password
	if info.User != "" {
		uri.WriteString(url.QueryEscape(info.User))
		if info.Password != "" {
			uri.WriteString(":")
			uri.WriteString(url.QueryEscape(info.Password))
		}
		uri.WriteString("@")
	}

	// Host and port with tcp protocol
	if info.Host != "" {
		uri.WriteString("tcp(")
		uri.WriteString(info.Host)
		if info.Port > 0 {
			uri.WriteString(fmt.Sprintf(":%d", info.Port))
		}
		uri.WriteString(")")
	}

	// Database
	if info.Database != "" {
		uri.WriteString("/")
		uri.WriteString(info.Database)
	}

	result := uri.String()
	return result
}
