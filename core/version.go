package core

import (
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/spf13/cast"
)

// Version is the version number
var Version = "dev"

var TelProps = g.M(
	"application", "sling-cli",
	"version", Version,
	"os", runtime.GOOS+"/"+runtime.GOARCH,
)

func init() {
	// dev build version is in format => 1.2.2.dev/2024-08-20
	parts := strings.Split(Version, "/")
	if len(parts) != 2 {
		return
	}

	// check expiration date for dev build (30 day window)
	if date := cast.ToTime(parts[1]); !date.IsZero() && date.Add(30*24*time.Hour).Before(time.Now()) {
		g.Warn("Sling dev build (%s) has expired! Please download the latest version at https://slingdata.io", parts[0])
		os.Exit(5)
	}

	// update version string
	Version = g.F("%s (%s)", parts[0], parts[1])
}
