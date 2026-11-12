// Package landing names and writes raw CFBD API responses in the lakehouse landing
// bucket, following the shared contract:
//
//	gs://<proj>-sa-landing/cfb/<endpoint>/season=<YYYY>/week=<WW>/<fetched_at>.json.gz
//
// Each object is one gzipped API response, written with an ifGenerationMatch=0
// precondition so nothing is ever overwritten, and carries the custom metadata
// endpoint, params, params_hash, fetched_at and http_status. The idempotency key is
// (endpoint, params_hash).
package landing

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Metadata keys every landed object carries.
const (
	MetaEndpoint   = "endpoint"
	MetaParams     = "params"
	MetaParamsHash = "params_hash"
	MetaFetchedAt  = "fetched_at"
	MetaHTTPStatus = "http_status"
)

// ContentType of landed objects. Deliberately not Content-Encoding: gzip, so readers
// (the Spark GCS connector included) get the stored bytes and decompress by the .gz
// extension instead of relying on GCS decompressive transcoding.
const ContentType = "application/gzip"

// fetchedAtLayout is <fetched_at_iso> in object names: UTC, YYYYMMDDTHHMMSSZ.
const fetchedAtLayout = "20060102T150405Z"

// ErrExists is returned by Store.Create when the object name is already taken.
var ErrExists = errors.New("landing object already exists")

// Object is a landed object's name and custom metadata.
type Object struct {
	Name     string
	Metadata map[string]string
}

// Store lists and creates landing objects.
type Store interface {
	// List returns every object whose name starts with prefix, with its metadata.
	List(ctx context.Context, prefix string) ([]Object, error)
	// Create writes a new object only if no object with that name exists
	// (ifGenerationMatch=0); otherwise it returns ErrExists.
	Create(ctx context.Context, name string, data []byte, contentType string, metadata map[string]string) error
}

// EndpointName maps an API path to its landing name: no leading "/", every other "/"
// replaced by "_" (e.g. "/games/teams" becomes "games_teams").
func EndpointName(apiPath string) string {
	return strings.ReplaceAll(strings.TrimPrefix(apiPath, "/"), "/", "_")
}

// CanonicalParams is the canonical query string: keys sorted, values URL-encoded.
func CanonicalParams(q url.Values) string {
	return q.Encode() // url.Values.Encode sorts by key
}

// ParamsHash is the hex SHA-256 of the canonical query string.
func ParamsHash(params string) string {
	sum := sha256.Sum256([]byte(params))
	return hex.EncodeToString(sum[:])
}

// SeasonPrefix is the listing prefix for one endpoint and season.
func SeasonPrefix(endpoint string, season int) string {
	return fmt.Sprintf("cfb/%s/season=%04d/", endpoint, season)
}

// ObjectName returns the object name of a response. week is 0 for season-level
// endpoints (week=00).
func ObjectName(endpoint string, season, week int, fetchedAt time.Time) string {
	return fmt.Sprintf("%sweek=%02d/%s.json.gz", SeasonPrefix(endpoint, season), week, fetchedAt.UTC().Format(fetchedAtLayout))
}

// Metadata returns the custom metadata of a response.
func Metadata(endpoint string, q url.Values, fetchedAt time.Time, httpStatus int) map[string]string {
	params := CanonicalParams(q)
	return map[string]string{
		MetaEndpoint:   endpoint,
		MetaParams:     params,
		MetaParamsHash: ParamsHash(params),
		MetaFetchedAt:  fetchedAt.UTC().Format(time.RFC3339),
		MetaHTTPStatus: strconv.Itoa(httpStatus),
	}
}
