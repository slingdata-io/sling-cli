//go:build network

package assist

import (
	"net/http"
	"testing"
	"time"
)

// Optional live check that the pinned GitHub asset exists.
// Default `go test ./core/sling/assist/` stays offline (this file is tagged).
func TestPinnedOpenCodeReleaseAssetExists(t *testing.T) {
	openCodeTestDownloadURL = ""
	u, err := openCodeDownloadURL(OpenCodeVersion)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 15 * time.Second, CheckRedirect: func(req *http.Request, via []*http.Request) error {
		return nil
	}}
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Range", "bytes=0-0")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusFound && resp.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("pinned asset %s: HTTP %d", u, resp.StatusCode)
	}
}
