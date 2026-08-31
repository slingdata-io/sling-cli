package evals

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const (
	fixtureToken        = "eval-token"
	fixtureClientID     = "eval-client"
	fixtureClientSecret = "eval-secret"
	fixtureOAuthToken   = "oauth-eval-token"
	fixtureUserCount    = 250
	fixtureOrderCount   = 1000
	fixtureUserPage     = 50
	fixtureOrderPage    = 100
)

// FixtureServer is the hermetic API used by spec cases.
type FixtureServer struct {
	URL      string
	listener net.Listener
	srv      *http.Server
	flaky    atomic.Int32
}

// StartFixtureServer binds 127.0.0.1:0 and serves fixture routes.
func StartFixtureServer() (*FixtureServer, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	fs := &FixtureServer{listener: ln, URL: "http://" + ln.Addr().String()}
	mux := http.NewServeMux()
	mux.HandleFunc("/oauth/token", fs.handleToken)
	mux.HandleFunc("/users/", fs.handleUserChild)
	mux.HandleFunc("/users", fs.handleUsers)
	mux.HandleFunc("/orders", fs.handleOrders)
	mux.HandleFunc("/flaky", fs.handleFlaky)
	mux.HandleFunc("/sample.csv", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/csv")
		w.Write([]byte("id,name\n1,alice\n2,bob\n"))
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})
	fs.srv = &http.Server{Handler: mux}
	go func() { _ = fs.srv.Serve(ln) }()
	return fs, nil
}

func (fs *FixtureServer) Close() {
	if fs == nil || fs.srv == nil {
		return
	}
	_ = fs.srv.Close()
}

func (fs *FixtureServer) authOK(r *http.Request) bool {
	h := r.Header.Get("Authorization")
	return h == "Bearer "+fixtureToken || h == "Bearer "+fixtureOAuthToken
}

func (fs *FixtureServer) handleToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	_ = r.ParseForm()
	id := r.Form.Get("client_id")
	sec := r.Form.Get("client_secret")
	if id == "" {
		id = r.Header.Get("X-Client-Id")
	}
	if id == "" {
		// JSON body
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		id = fmt.Sprint(body["client_id"])
		sec = fmt.Sprint(body["client_secret"])
	}
	if id != fixtureClientID || sec != fixtureClientSecret {
		http.Error(w, `{"error":"invalid_client"}`, http.StatusUnauthorized)
		return
	}
	writeJSON(w, map[string]any{
		"access_token": fixtureOAuthToken,
		"token_type":   "Bearer",
		"expires_in":   3600,
	})
}

func (fs *FixtureServer) handleUsers(w http.ResponseWriter, r *http.Request) {
	if !fs.authOK(r) {
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
		return
	}
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = fixtureUserPage
	}
	since := r.URL.Query().Get("updated_since")
	users := makeUsers()
	if since != "" {
		cut, err := time.Parse(time.RFC3339, since)
		if err == nil {
			var filtered []map[string]any
			for _, u := range users {
				ts, _ := time.Parse(time.RFC3339, u["updated_at"].(string))
				if ts.After(cut) {
					filtered = append(filtered, u)
				}
			}
			users = filtered
		}
	}
	if offset > len(users) {
		offset = len(users)
	}
	end := offset + limit
	if end > len(users) {
		end = len(users)
	}
	writeJSON(w, map[string]any{
		"data":   users[offset:end],
		"offset": offset,
		"limit":  limit,
		"total":  len(users),
	})
}

func (fs *FixtureServer) handleUserChild(w http.ResponseWriter, r *http.Request) {
	if !fs.authOK(r) {
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
		return
	}
	// /users/{id}/posts
	rest := strings.TrimPrefix(r.URL.Path, "/users/")
	parts := strings.Split(strings.Trim(rest, "/"), "/")
	if len(parts) < 2 || parts[1] != "posts" {
		http.NotFound(w, r)
		return
	}
	id, _ := strconv.Atoi(parts[0])
	if id < 1 || id > fixtureUserCount {
		http.NotFound(w, r)
		return
	}
	posts := []map[string]any{
		{"id": id*10 + 1, "user_id": id, "title": fmt.Sprintf("post-%d-a", id)},
		{"id": id*10 + 2, "user_id": id, "title": fmt.Sprintf("post-%d-b", id)},
	}
	writeJSON(w, map[string]any{"data": posts})
}

func (fs *FixtureServer) handleOrders(w http.ResponseWriter, r *http.Request) {
	if !fs.authOK(r) {
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
		return
	}
	cur, _ := strconv.Atoi(r.URL.Query().Get("cursor"))
	if cur < 0 {
		cur = 0
	}
	end := cur + fixtureOrderPage
	var next any
	if end < fixtureOrderCount {
		next = strconv.Itoa(end)
	} else {
		end = fixtureOrderCount
		next = nil
	}
	var rows []map[string]any
	for i := cur; i < end; i++ {
		rows = append(rows, map[string]any{
			"id":         i + 1,
			"user_id":    1 + (i % fixtureUserCount),
			"total":      10 + i%90,
			"updated_at": orderTime(i).Format(time.RFC3339),
		})
	}
	writeJSON(w, map[string]any{"data": rows, "next_cursor": next})
}

func (fs *FixtureServer) handleFlaky(w http.ResponseWriter, r *http.Request) {
	if !fs.authOK(r) {
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
		return
	}
	n := fs.flaky.Add(1)
	if n == 1 {
		w.Header().Set("Retry-After", "0")
		http.Error(w, `{"error":"rate limited"}`, http.StatusTooManyRequests)
		return
	}
	writeJSON(w, map[string]any{"data": []map[string]any{{"id": 1, "ok": true}}})
}

func makeUsers() []map[string]any {
	out := make([]map[string]any, fixtureUserCount)
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < fixtureUserCount; i++ {
		out[i] = map[string]any{
			"id":         i + 1,
			"name":       fmt.Sprintf("user-%03d", i+1),
			"email":      fmt.Sprintf("user%d@example.com", i+1),
			"updated_at": base.Add(time.Duration(i) * time.Hour).Format(time.RFC3339),
		}
	}
	return out
}

func orderTime(i int) time.Time {
	return time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(i) * time.Minute)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
