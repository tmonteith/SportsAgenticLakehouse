package landing

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestEndpointName(t *testing.T) {
	cases := map[string]string{"/games": "games", "/plays": "plays", "/teams": "teams", "/games/teams": "games_teams"}
	for in, want := range cases {
		if got := EndpointName(in); got != want {
			t.Errorf("EndpointName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestObjectNameAndMetadata(t *testing.T) {
	at := time.Date(2024, 9, 1, 9, 30, 5, 123e6, time.FixedZone("MDT", -6*3600))
	if got, want := ObjectName("games", 2024, 1, at), "cfb/games/season=2024/week=01/20240901T153005Z.json.gz"; got != want {
		t.Errorf("ObjectName = %q, want %q", got, want)
	}
	if got, want := ObjectName("teams", 2024, 0, at), "cfb/teams/season=2024/week=00/20240901T153005Z.json.gz"; got != want {
		t.Errorf("season-level ObjectName = %q, want %q", got, want)
	}

	md := Metadata("games", url.Values{"year": {"2024"}, "week": {"1"}, "seasonType": {"regular"}}, at, 200)
	want := map[string]string{
		"endpoint":   "games",
		"params":     "seasonType=regular&week=1&year=2024",
		"fetched_at": "2024-09-01T15:30:05Z",
		// sha256 of "seasonType=regular&week=1&year=2024"
		"params_hash": ParamsHash("seasonType=regular&week=1&year=2024"),
		"http_status": "200",
	}
	for k, v := range want {
		if md[k] != v {
			t.Errorf("metadata[%s] = %q, want %q", k, md[k], v)
		}
	}
	if len(md) != len(want) {
		t.Errorf("metadata has %d keys, want %d: %v", len(md), len(want), md)
	}
	if h := ParamsHash(""); h != "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" {
		t.Errorf("ParamsHash is not hex sha256: %s", h)
	}
}

func TestMemStoreNeverOverwrites(t *testing.T) {
	m := NewMemStore()
	ctx := context.Background()
	if err := m.Create(ctx, "cfb/a/x", []byte("1"), ContentType, nil); err != nil {
		t.Fatal(err)
	}
	if err := m.Create(ctx, "cfb/a/x", []byte("2"), ContentType, nil); !errors.Is(err, ErrExists) {
		t.Fatalf("second create err = %v, want ErrExists", err)
	}
	if d, _, _, _ := m.Get("cfb/a/x"); string(d) != "1" {
		t.Errorf("object overwritten: %q", d)
	}
}

// fakeGCS implements just enough of the JSON API: paged list and multipart insert
// with ifGenerationMatch=0.
type fakeGCS struct {
	t        *testing.T
	objects  map[string]map[string]string
	inserts  int
	fail5xx  int // number of upcoming inserts that answer 503 after storing (lost response)
	pageSize int
}

func (f *fakeGCS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/storage/v1/b/test-bucket/o":
		q := r.URL.Query()
		if q.Get("fields") != "items(name,metadata),nextPageToken" {
			f.t.Errorf("list fields = %q", q.Get("fields"))
		}
		var names []string
		for n := range f.objects {
			if strings.HasPrefix(n, q.Get("prefix")) {
				names = append(names, n)
			}
		}
		sort.Strings(names)
		start := 0
		if tok := q.Get("pageToken"); tok != "" {
			for i, n := range names {
				if n == tok {
					start = i
				}
			}
		}
		end := start + f.pageSize
		resp := map[string]any{}
		if end < len(names) {
			resp["nextPageToken"] = names[end]
		} else {
			end = len(names)
		}
		var items []map[string]any
		for _, n := range names[start:end] {
			items = append(items, map[string]any{"name": n, "metadata": f.objects[n]})
		}
		if items != nil {
			resp["items"] = items
		}
		_ = json.NewEncoder(w).Encode(resp)

	case r.Method == http.MethodPost && r.URL.Path == "/upload/storage/v1/b/test-bucket/o":
		q := r.URL.Query()
		if q.Get("uploadType") != "multipart" || q.Get("ifGenerationMatch") != "0" {
			f.t.Errorf("insert query = %s", r.URL.RawQuery)
		}
		mediaType, params, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if mediaType != "multipart/related" {
			f.t.Errorf("content type = %s", mediaType)
		}
		mr := multipart.NewReader(r.Body, params["boundary"])
		p1, _ := mr.NextPart()
		var meta struct {
			Name        string            `json:"name"`
			ContentType string            `json:"contentType"`
			Metadata    map[string]string `json:"metadata"`
		}
		_ = json.NewDecoder(p1).Decode(&meta)
		p2, _ := mr.NextPart()
		data, _ := io.ReadAll(p2)
		if p2.Header.Get("Content-Type") != meta.ContentType || string(data) != "gz-bytes" {
			f.t.Errorf("media part: %s %q", p2.Header.Get("Content-Type"), data)
		}
		if _, ok := f.objects[meta.Name]; ok {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		f.objects[meta.Name] = meta.Metadata
		f.inserts++
		if f.fail5xx > 0 {
			f.fail5xx--
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"name":"x","generation":"1"}`))
	default:
		f.t.Errorf("unexpected request %s %s", r.Method, r.URL)
		w.WriteHeader(http.StatusNotFound)
	}
}

func newFakeGCS(t *testing.T) (*fakeGCS, *GCS) {
	f := &fakeGCS{t: t, objects: map[string]map[string]string{}, pageSize: 2}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	g := NewGCSWithClient("test-bucket", srv.Client(), srv.URL)
	g.sleep = func(context.Context, time.Duration) error { return nil }
	return f, g
}

func TestGCSCreateAndList(t *testing.T) {
	f, g := newFakeGCS(t)
	ctx := context.Background()
	names := []string{
		"cfb/games/season=2024/week=01/20240901T000000Z.json.gz",
		"cfb/games/season=2024/week=01/20240901T000001Z.json.gz",
		"cfb/games/season=2024/week=02/20240908T000000Z.json.gz",
		"cfb/plays/season=2024/week=01/20240901T000002Z.json.gz",
	}
	for i, n := range names {
		if err := g.Create(ctx, n, []byte("gz-bytes"), ContentType, map[string]string{"params_hash": string(rune('a' + i))}); err != nil {
			t.Fatalf("Create %s: %v", n, err)
		}
	}
	if err := g.Create(ctx, names[0], []byte("gz-bytes"), ContentType, nil); !errors.Is(err, ErrExists) {
		t.Fatalf("duplicate Create err = %v, want ErrExists", err)
	}

	objs, err := g.List(ctx, "cfb/games/season=2024/")
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != 3 || objs[2].Metadata["params_hash"] != "c" {
		t.Fatalf("List across pages = %+v", objs)
	}
	if f.inserts != 4 {
		t.Errorf("inserts = %d", f.inserts)
	}
}

func TestGCSCreateRetryAfterLostResponse(t *testing.T) {
	f, g := newFakeGCS(t)
	f.fail5xx = 1 // stored, but the client sees 503 and retries into a 412
	if err := g.Create(context.Background(), "cfb/teams/season=2024/week=00/x.json.gz", []byte("gz-bytes"), ContentType, nil); err != nil {
		t.Fatalf("Create after lost response = %v, want success", err)
	}
	if f.inserts != 1 {
		t.Errorf("inserts = %d, want 1", f.inserts)
	}
}
