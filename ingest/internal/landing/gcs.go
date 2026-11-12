package landing

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"time"

	"golang.org/x/oauth2/google"
)

// The extractor needs to list, read metadata and create objects. Its service account
// holds roles/storage.objectCreator and roles/storage.objectViewer on the landing
// bucket only; the scope does not widen that.
const storageScope = "https://www.googleapis.com/auth/devstorage.read_write"

const defaultStorageBase = "https://storage.googleapis.com"

// GCS is a Store backed by the Cloud Storage JSON API. It uses only list and
// multipart insert, so it needs no client library beyond OAuth2.
type GCS struct {
	bucket string
	base   string
	hc     *http.Client
	sleep  func(ctx context.Context, d time.Duration) error
}

// NewGCS returns a GCS store for bucket authenticated with Application Default
// Credentials (GOOGLE_APPLICATION_CREDENTIALS on OKE). A later switch to a workload
// identity credential configuration file needs no code change.
func NewGCS(ctx context.Context, bucket string) (*GCS, error) {
	hc, err := google.DefaultClient(ctx, storageScope)
	if err != nil {
		return nil, fmt.Errorf("application default credentials: %w", err)
	}
	return NewGCSWithClient(bucket, hc, defaultStorageBase), nil
}

// NewGCSWithClient returns a GCS store using hc against base (tests point it at a fake).
func NewGCSWithClient(bucket string, hc *http.Client, base string) *GCS {
	return &GCS{bucket: bucket, base: base, hc: hc, sleep: sleepCtx}
}

type listResponse struct {
	Items []struct {
		Name     string            `json:"name"`
		Metadata map[string]string `json:"metadata"`
	} `json:"items"`
	NextPageToken string `json:"nextPageToken"`
}

// List pages through objects.list with only the fields the extractor needs.
func (g *GCS) List(ctx context.Context, prefix string) ([]Object, error) {
	var out []Object
	pageToken := ""
	for {
		q := url.Values{
			"prefix":     {prefix},
			"fields":     {"items(name,metadata),nextPageToken"},
			"maxResults": {"1000"},
		}
		if pageToken != "" {
			q.Set("pageToken", pageToken)
		}
		u := fmt.Sprintf("%s/storage/v1/b/%s/o?%s", g.base, url.PathEscape(g.bucket), q.Encode())

		var page listResponse
		err := g.withRetry(ctx, func() (int, error) {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
			if err != nil {
				return 0, err
			}
			resp, err := g.hc.Do(req)
			if err != nil {
				return 0, err
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return resp.StatusCode, statusErr("list", prefix, resp)
			}
			page = listResponse{}
			return resp.StatusCode, json.NewDecoder(resp.Body).Decode(&page)
		})
		if err != nil {
			return nil, err
		}
		for _, it := range page.Items {
			out = append(out, Object{Name: it.Name, Metadata: it.Metadata})
		}
		if page.NextPageToken == "" {
			return out, nil
		}
		pageToken = page.NextPageToken
	}
}

// Create uploads data as a new object with a multipart insert and
// ifGenerationMatch=0. A 412 means the name is taken: ErrExists on the first attempt.
// On a retry it means the earlier attempt landed before its response was lost (the name
// carries the fetch second, endpoint, season and week), so it counts as success.
func (g *GCS) Create(ctx context.Context, name string, data []byte, contentType string, metadata map[string]string) error {
	meta, err := json.Marshal(map[string]any{
		"name":        name,
		"contentType": contentType,
		"metadata":    metadata,
	})
	if err != nil {
		return err
	}

	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	if err := writePart(mw, "application/json; charset=UTF-8", meta); err != nil {
		return err
	}
	if err := writePart(mw, contentType, data); err != nil {
		return err
	}
	if err := mw.Close(); err != nil {
		return err
	}
	payload := body.Bytes()

	q := url.Values{"uploadType": {"multipart"}, "ifGenerationMatch": {"0"}, "fields": {"name,generation"}}
	u := fmt.Sprintf("%s/upload/storage/v1/b/%s/o?%s", g.base, url.PathEscape(g.bucket), q.Encode())

	attempt := 0
	return g.withRetry(ctx, func() (int, error) {
		attempt++
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(payload))
		if err != nil {
			return 0, err
		}
		req.Header.Set("Content-Type", "multipart/related; boundary="+mw.Boundary())
		resp, err := g.hc.Do(req)
		if err != nil {
			return 0, err
		}
		defer resp.Body.Close()
		switch {
		case resp.StatusCode == http.StatusOK:
			_, _ = io.Copy(io.Discard, resp.Body)
			return resp.StatusCode, nil
		case resp.StatusCode == http.StatusPreconditionFailed && attempt > 1:
			return resp.StatusCode, nil
		case resp.StatusCode == http.StatusPreconditionFailed:
			return resp.StatusCode, fmt.Errorf("%s: %w", name, ErrExists)
		default:
			return resp.StatusCode, statusErr("insert", name, resp)
		}
	})
}

func writePart(mw *multipart.Writer, contentType string, data []byte) error {
	h := textproto.MIMEHeader{}
	h.Set("Content-Type", contentType)
	w, err := mw.CreatePart(h)
	if err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

// withRetry runs op up to 4 times, retrying transport errors, 429 and 5xx with 1s, 2s,
// 4s backoff. Both operations are safe to repeat (list is read-only, insert has a
// generation precondition).
func (g *GCS) withRetry(ctx context.Context, op func() (status int, err error)) error {
	var err error
	for attempt := 0; attempt < 4; attempt++ {
		if attempt > 0 {
			if serr := g.sleep(ctx, time.Duration(1<<(attempt-1))*time.Second); serr != nil {
				return serr
			}
		}
		var status int
		status, err = op()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		retryable := status == 0 || status == http.StatusTooManyRequests || status >= 500
		if !retryable {
			return err
		}
	}
	return err
}

func statusErr(op, name string, resp *http.Response) error {
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 300))
	return fmt.Errorf("GCS %s %s: %s: %s", op, name, resp.Status, bytes.TrimSpace(b))
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
