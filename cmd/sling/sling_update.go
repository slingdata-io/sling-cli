package main

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"log"
	"os"
	"path"
	"runtime"
	"strings"

	"github.com/flarco/g"
	"github.com/flarco/g/net"
	"github.com/kardianos/osext"
	"github.com/slingdata-io/sling-cli/core"
	"github.com/spf13/cast"
)

func updateCLI(c *g.CliSC) (ok bool, err error) {
	// Print Progress: https://gist.github.com/albulescu/e61979cc852e4ee8f49c

	ok = true
	telemetryMap["downloaded"] = false

	// get latest version number
	checkUpdate()
	if updateVersion == core.Version {
		g.Info("Already up-to-date!")
		return
	}

	telemetryMap["new_version"] = updateVersion
	url := ""
	if runtime.GOOS == "linux" {
		url = "https://github.com/slingdata-io/sling-cli/releases/latest/download/sling_linux_amd64.tar.gz"
	} else if runtime.GOOS == "darwin" {
		if runtime.GOARCH == "amd64" {
			url = "https://github.com/slingdata-io/sling-cli/releases/latest/download/sling_darwin_amd64.tar.gz"
		} else {
			url = "https://github.com/slingdata-io/sling-cli/releases/latest/download/sling_darwin_arm64.tar.gz"
		}
	} else if runtime.GOOS == "windows" {
		url = "https://github.com/slingdata-io/sling-cli/releases/latest/download/sling_windows_amd64.tar.gz"
	} else {
		return ok, g.Error("OS Unsupported: %s", runtime.GOOS)
	}

	execFileName, err := osext.Executable()
	if err != nil {
		return ok, g.Error(err, "Unable to determine executable path")
	} else if strings.Contains(execFileName, "homebrew") {
		g.Warn("Sling was installed with brew, please run `brew upgrade slingdata-io/sling/sling`")
		return ok, nil
	} else if strings.Contains(execFileName, "scoop") {
		g.Warn("Sling was installed with scoop, please run `scoop update sling`")
		return ok, nil
	}

	fileStat, _ := os.Stat(execFileName)
	fileMode := fileStat.Mode()

	folderPath := path.Join(os.TempDir(), "sling.new")
	err = os.MkdirAll(folderPath, 0777)
	if err != nil {
		return ok, g.Error(err, "could not create temp folder")
	}

	tazGzFilePath := path.Join(folderPath, "sling.tar.gz")

	g.Info("Downloading latest version (%s)", updateVersion)
	err = net.DownloadFile(url, tazGzFilePath)
	if err != nil {
		println("Unable to download update!")
		return ok, g.Error(strings.ReplaceAll(err.Error(), url, ""))
	}

	telemetryMap["downloaded"] = true

	// expand archive
	err = ExtractTarGz(tazGzFilePath, folderPath)
	if err != nil {
		println("Unable to download update!")
		return ok, err
	}

	filePath := path.Join(folderPath, "sling")
	if runtime.GOOS == "windows" {
		filePath = filePath + ".exe"
	}
	err = os.Chmod(filePath, fileMode)
	if err != nil {
		println("Unable to make new binary executable.")
		return ok, err
	}

	err = os.Rename(execFileName, execFileName+".old")
	if err != nil {
		println("Unable to rename current binary executable. Try with sudo or admin?")
		return ok, err
	}

	err = os.Rename(filePath, execFileName)
	if err != nil {
		println("Unable to rename current binary executable. Try with sudo or admin?")
		return ok, err
	}

	os.Rename(execFileName+".old", filePath+".old")
	os.RemoveAll(folderPath)

	g.Info("Updated to " + strings.TrimSpace(string(updateVersion)))

	return ok, nil
}

func checkUpdate() {
	if core.Version == "dev" {
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
	slingPackage := ""
	execFileName, _ := osext.Executable()
	switch {
	case isDocker:
		slingPackage = "docker"
	case strings.Contains(execFileName, "homebrew"):
		slingPackage = "homebrew"
	case strings.Contains(execFileName, "scoop"):
		slingPackage = "scoop"
	case strings.Contains(execFileName, "python"):
		slingPackage = "python"
	default:
		slingPackage = "binary"
	}
	return slingPackage
}

func ExtractTarGz(filePath, outFolder string) (err error) {
	gzipStream, err := os.Open(filePath)
	if err != nil {
		return g.Error(err, "could not open file")
	}
	uncompressedStream, err := gzip.NewReader(gzipStream)
	if err != nil {
		log.Fatal("ExtractTarGz: NewReader failed")
	}

	tarReader := tar.NewReader(uncompressedStream)

	for {
		header, err := tarReader.Next()

		if err == io.EOF {
			break
		}

		if err != nil {
			log.Fatalf("ExtractTarGz: Next() failed: %s", err.Error())
			return g.Error(
				err,
				"ExtractTarGz: Next() failed",
				header.Typeflag,
				header.Name)
		}

		outPath := path.Join(outFolder, header.Name)
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.Mkdir(outPath, 0755); err != nil {
				log.Fatalf("ExtractTarGz: Mkdir() failed: %s", err.Error())
			}
		case tar.TypeReg:
			outFile, err := os.Create(outPath)
			if err != nil {
				log.Fatalf("ExtractTarGz: Create() failed: %s", err.Error())
			}
			if _, err := io.Copy(outFile, tarReader); err != nil {
				log.Fatalf("ExtractTarGz: Copy() failed: %s", err.Error())
			}
			outFile.Close()

		default:
			return g.Error(
				"ExtractTarGz: uknown type: %s in %s",
				header.Typeflag,
				header.Name)
		}
	}

	return nil
}
