package marker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tmonteith/SportsAgenticLakehouse/ingest/internal/landing"
)

func quiet() *slog.Logger { return slog.New(slog.NewJSONHandler(io.Discard, nil)) }

func TestObjectNameUsesDenverDateOfRunStart(t *testing.T) {
	cases := []struct {
		start string
		want  string
	}{
		// 03:30 MST (UTC-7 in winter) is 10:30 UTC the same day.
		{"2026-11-14T10:30:00Z", "_markers/extractor-nightly/2026-11-14.json"},
		// 05:30 UTC is still the evening before in Denver (MST).
		{"2026-11-14T05:30:00Z", "_markers/extractor-nightly/2026-11-13.json"},
		// Summer (MDT, UTC-6): 05:59 UTC is 23:59 the day before; 06:00 UTC is midnight.
		{"2026-07-04T05:59:59Z", "_markers/extractor-nightly/2026-07-03.json"},
		{"2026-07-04T06:00:00Z", "_markers/extractor-nightly/2026-07-04.json"},
	}
	for _, c := range cases {
		start, _ := time.Parse(time.RFC3339, c.start)
		got, err := ObjectName("extractor-nightly", start)
		if err != nil || got != c.want {
			t.Errorf("ObjectName(%s) = %q, %v; want %q", c.start, got, err, c.want)
		}
	}
}

func TestValidateName(t *testing.T) {
	for _, ok := range []string{"extractor-nightly", "a", "x.y_z-1"} {
		if err := ValidateName(ok); err != nil {
			t.Errorf("ValidateName(%q) = %v", ok, err)
		}
	}
	for _, bad := range []string{"", "Extractor", "a/b", "-x", "../x", "a b"} {
		if err := ValidateName(bad); err == nil {
			t.Errorf("ValidateName(%q) accepted", bad)
		}
	}
}

var start = time.Date(2026, 11, 14, 10, 30, 0, 0, time.UTC)

func body() Body {
	return NewBody(start, start.Add(95*time.Second), 12, 14, "abc1234")
}

func TestAfterRunWritesMarker(t *testing.T) {
	store := landing.NewMemStore()
	res, err := AfterRun(context.Background(), store, "extractor-nightly", start, false, nil, body(), quiet())
	if err != nil || res != Written {
		t.Fatalf("AfterRun = %v, %v", res, err)
	}
	data, ct, meta, ok := store.Get("_markers/extractor-nightly/2026-11-14.json")
	if !ok {
		t.Fatalf("marker not written; objects: %v", store.Names())
	}
	if ct != "application/json" || meta["marker"] != "extractor-nightly" {
		t.Errorf("content type %q, metadata %v", ct, meta)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"started_at": "2026-11-14T10:30:00Z", "finished_at": "2026-11-14T10:31:35Z",
		"files_written": 12.0, "api_calls": 14.0, "image_tag": "abc1234",
	}
	if len(got) != len(want) {
		t.Errorf("body keys = %v", got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("body[%s] = %v, want %v", k, got[k], v)
		}
	}
	if strings.HasPrefix(store.Names()[0], "cfb/") {
		t.Errorf("marker must sit outside cfb/")
	}
}

func TestAfterRunNeverWritesOnFailureOrDryRun(t *testing.T) {
	for _, c := range []struct {
		name   string
		dryRun bool
		runErr error
	}{
		{"failed", false, errors.New("2 request(s) failed")},
		{"dry run", true, nil},
		{"failed dry run", true, errors.New("boom")},
	} {
		store := landing.NewMemStore()
		res, err := AfterRun(context.Background(), store, "extractor-nightly", start, c.dryRun, c.runErr, body(), quiet())
		if err != nil || res != Skipped || len(store.Names()) != 0 {
			t.Errorf("%s: AfterRun = %v, %v; objects %v", c.name, res, err, store.Names())
		}
	}
}

func TestAfterRunDisabledWithoutName(t *testing.T) {
	store := landing.NewMemStore()
	res, err := AfterRun(context.Background(), store, "", start, false, nil, body(), quiet())
	if err != nil || res != Disabled || len(store.Names()) != 0 {
		t.Fatalf("AfterRun = %v, %v; objects %v", res, err, store.Names())
	}
}

func TestAfterRunKeepsExistingMarker(t *testing.T) {
	store := landing.NewMemStore()
	first := body()
	if _, err := AfterRun(context.Background(), store, "extractor-nightly", start, false, nil, first, quiet()); err != nil {
		t.Fatal(err)
	}
	// A second successful run the same Denver day keeps the first marker.
	later := start.Add(3 * time.Hour)
	second := NewBody(later, later.Add(time.Minute), 1, 2, "def5678")
	var logs bytes.Buffer
	res, err := AfterRun(context.Background(), store, "extractor-nightly", later, false, nil, second, slog.New(slog.NewJSONHandler(&logs, nil)))
	if err != nil || res != Kept {
		t.Fatalf("AfterRun = %v, %v", res, err)
	}
	if !strings.Contains(logs.String(), `"msg":"marker_exists"`) {
		t.Errorf("existing marker not logged: %s", logs.String())
	}
	data, _, _, _ := store.Get("_markers/extractor-nightly/2026-11-14.json")
	if !strings.Contains(string(data), `"image_tag":"abc1234"`) {
		t.Errorf("existing marker was replaced: %s", data)
	}
}

// The GCS store sends ifGenerationMatch=0 and maps a 412 to "kept".
func TestAfterRunAgainstGCS(t *testing.T) {
	var queries []string
	exists := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries = append(queries, r.URL.RawQuery)
		if exists {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		exists = true
		_, _ = w.Write([]byte(`{"name":"x","generation":"1"}`))
	}))
	defer srv.Close()
	store := landing.NewGCSWithClient("proj-sa-landing", srv.Client(), srv.URL)

	res, err := AfterRun(context.Background(), store, "extractor-nightly", start, false, nil, body(), quiet())
	if err != nil || res != Written {
		t.Fatalf("first: %v, %v", res, err)
	}
	res, err = AfterRun(context.Background(), store, "extractor-nightly", start, false, nil, body(), quiet())
	if err != nil || res != Kept {
		t.Fatalf("second: %v, %v", res, err)
	}
	for _, q := range queries {
		if !strings.Contains(q, "ifGenerationMatch=0") {
			t.Errorf("insert without ifGenerationMatch=0: %s", q)
		}
	}
}

func TestAfterRunReportsWriteFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()
	store := landing.NewGCSWithClient("proj-sa-landing", srv.Client(), srv.URL)
	if _, err := AfterRun(context.Background(), store, "extractor-nightly", start, false, nil, body(), quiet()); err == nil {
		t.Fatal("a failed marker write must be an error")
	}
}
