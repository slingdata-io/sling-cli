package main

import (
	"os"
	"path"
	"runtime"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/flarco/g/net"
	"github.com/flarco/g/process"
	"github.com/kardianos/osext"
	"github.com/slingdata-io/sling-cli/core"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
)

// getDownloadURL returns the appropriate download URL based on the channel (dev or stable)
func getDownloadURL(isDevChannel bool) string {
	baseURL := "https://github.com/slingdata-io/sling-cli/releases/latest/download"
	if isDevChannel {
		baseURL = "https://f.slingdata.io/dev/latest"
	}

	osName := runtime.GOOS
	arch := runtime.GOARCH

	if !g.In(osName, "linux", "darwin", "windows") {
		return "" // unsupported OS
	}

	if arch != "amd64" {
		arch = "arm64" // assume arm64 if not amd64
	}

	if isDevChannel && osName == "windows" {
		arch = "amd64" // only amd64 for windows dev channel
	}

	return g.F("%s/sling_%s_%s.tar.gz", baseURL, osName, arch)
}

func updateCLI(c *g.CliSC) (ok bool, err error) {
	// Print Progress: https://gist.github.com/albulescu/e61979cc852e4ee8f49c

	ok = true
	env.TelMap["downloaded"] = false

	// detect if running dev or stable channel
	isDevChannel := strings.Contains(core.Version, "dev")

	// get latest version number (skipped for dev channel)
	if !isDevChannel {
		checkUpdate(true)
		if updateVersion == core.Version {
			g.Info("Already up-to-date!")
			return
		}
		env.TelMap["new_version"] = updateVersion
	}

	// get download URL for the appropriate channel
	url := getDownloadURL(isDevChannel)
	if url == "" {
		return ok, g.Error("OS Unsupported: %s", runtime.GOOS)
	}

	execFileName, err := osext.Executable()
	if err != nil {
		return ok, g.Error(err, "Unable to determine executable path")
	} else if strings.Contains(execFileName, "homebrew") {
		if err = upgradeBrew(); err != nil {
			g.Warn("Could not auto-upgrade, please manually run `brew upgrade slingdata-io/sling/sling`")
		}
		return ok, nil
	} else if strings.Contains(execFileName, "scoop") {
		if err = upgradeScoop(); err != nil {
			g.Warn("Could not auto-upgrade, please manually run `scoop update sling`")
		}
		return ok, nil
	}

	fileStat, err := os.Stat(execFileName)
	if err != nil {
		return ok, g.Error(err, "could not stat %s", execFileName)
	}
	fileMode := fileStat.Mode()

	folderPath := path.Join(env.GetTempFolder(), "sling.new")
	err = os.MkdirAll(folderPath, 0777)
	if err != nil {
		return ok, g.Error(err, "could not create temp folder")
	}

	tazGzFilePath := path.Join(folderPath, "sling.tar.gz")

	// show appropriate message for dev vs stable channel
	if isDevChannel {
		g.Info("Updating to latest dev build...")
	} else {
		g.Info("Downloading latest version (%s)", updateVersion)
	}

	err = net.DownloadFile(url, tazGzFilePath)
	if err != nil {
		g.Warn("Unable to download update!")
		return ok, g.Error(strings.ReplaceAll(err.Error(), url, ""))
	}

	env.TelMap["downloaded"] = true

	// expand archive
	err = g.ExtractTarGz(tazGzFilePath, folderPath)
	if err != nil {
		g.Warn("Unable to download update!")
		return ok, err
	}

	filePath := path.Join(folderPath, "sling")
	if runtime.GOOS == "windows" {
		filePath = filePath + ".exe"
	}
	err = os.Chmod(filePath, fileMode)
	if err != nil {
		g.Warn("Unable to make new binary executable.")
		return ok, err
	}

	err = os.Rename(execFileName, execFileName+".old")
	if err != nil {
		g.Warn("Unable to rename current binary executable. Try with sudo or admin?")
		return ok, err
	}

	err = os.Rename(filePath, execFileName)
	if err != nil {
		g.Warn("Unable to rename current binary executable. Try with sudo or admin?")
		os.Rename(execFileName+".old", execFileName) // undo first rename
		return ok, err
	}

	os.Rename(execFileName+".old", filePath+".old")
	os.RemoveAll(folderPath)

	if isDevChannel {
		g.Info("Updated to latest dev build")
	} else {
		g.Info("Updated to " + strings.TrimSpace(string(updateVersion)))
	}

	return ok, nil
}

func upgradeBrew() (err error) {
	g.Info("Sling was installed with brew. Running `brew update` and `brew upgrade slingdata-io/sling/sling`")

	proc, err := process.NewProc("brew")
	if err != nil {
		return g.Error(err, "could not make brew process")
	}
	proc.Env = g.KVArrToMap(os.Environ()...)
	proc.Print = true

	if err = proc.Run("update"); err != nil {
		return g.Error(err, "could not update brew")
	}

	if err = proc.Run("upgrade", "slingdata-io/sling/sling"); err != nil {
		return g.Error(err, "could not upgrade sling via brew")
	}

	return nil
}

func upgradeScoop() (err error) {
	g.Warn("Sling was installed with scoop. Try running `scoop update sling`")
	return nil
	// errors with "sling" is still running. Need to install manually

	proc, err := process.NewProc("scoop")
	if err != nil {
		return g.Error(err, "could not make scoop process")
	}
	proc.Env = g.KVArrToMap(os.Environ()...)
	proc.Print = true

	if err = proc.Run("update", "sling"); err != nil {
		return g.Error(err, "could not update sling via scoop")
	}

	return nil
}

func checkUpdate(force bool) {
	if strings.Contains(core.Version, "dev") {
		return
	} else if time.Now().Second()%4 != 0 && !force {
		// a way to check/notify about a new version less frequently
		return
	}

	instruction := "Please run `sling update`"
	switch getSlingPackage() {
	case "homebrew":
		instruction = "Please run `brew upgrade slingdata-io/sling/sling`."
	case "scoop":
		instruction = "Please run `scoop update sling`."
	case "python":
		instruction = "Please run `pip install -U sling`."
	case "docker":
		instruction = "Please run `docker pull slingdata/sling` and recreate your container."
	}

	const url = "https://api.github.com/repos/slingdata-io/sling-cli/releases"
	_, respB, _ := net.ClientDo("GET", url, nil, nil)
	arr := []map[string]any{}
	g.JSONUnmarshal(respB, &arr)
	if len(arr) > 0 && arr[0] != nil {
		updateVersion = strings.TrimPrefix(cast.ToString(arr[0]["tag_name"]), "v")
		isNew, err := g.CompareVersions(core.Version, updateVersion)
		if err != nil {
			g.DebugLow("Error comparing versions: %s", err.Error())
		} else if isNew {
			updateMessage = g.F("FYI there is a new sling version released (%s). %s", updateVersion, instruction)
		}
	}
}

func getSlingPackage() string {
	slingPackage := strings.ToLower(os.Getenv("SLING_PACKAGE"))
	switch {
	case slingPackage != "":
		_ = slingPackage
	case os.Getenv("SLING_SOURCE") != "" && os.Getenv("SLING_TARGET") != "":
		slingPackage = "dagster"
	case strings.Contains(env.Executable, "homebrew"):
		slingPackage = "homebrew"
	case strings.Contains(env.Executable, "scoop"):
		slingPackage = "scoop"
	case strings.Contains(env.Executable, "python") || strings.Contains(env.Executable, "virtualenvs"):
		slingPackage = "python"
	default:
		slingPackage = "binary"
	}
	return slingPackage
}

func printUpdateAvailable() {
	if updateVersion != "" {
		println(updateMessage)
	}
}
