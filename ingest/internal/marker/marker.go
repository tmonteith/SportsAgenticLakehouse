// Package marker writes the extractor's completion marker (contract I2): after a
// successful nightly run, one JSON object at
//
//	gs://<proj>-sa-landing/_markers/<name>/<YYYY-MM-DD>.json
//
// where the date is the America/Denver date the run started. Airflow's cfb_nightly DAG
// waits for it before submitting the nightly Spark batch. The object sits outside cfb/,
// so load_landing never reads it. It is written with ifGenerationMatch=0: a marker that
// already exists (a second run the same day) is kept as it is.
package marker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"time"
	_ "time/tzdata" // the America/Denver rules, whatever the image ships

	"github.com/tmonteith/SportsAgenticLakehouse/ingest/internal/landing"
)

// Prefix is the folder of every marker in the landing bucket.
const Prefix = "_markers/"

// TimeZone decides the marker date.
const TimeZone = "America/Denver"

// ContentType of marker objects.
const ContentType = "application/json"

var validName = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*$`)

// Body is the marker's JSON content.
type Body struct {
	StartedAt    string `json:"started_at"`  // RFC 3339, UTC
	FinishedAt   string `json:"finished_at"` // RFC 3339, UTC
	FilesWritten int    `json:"files_written"`
	APICalls     int    `json:"api_calls"`
	ImageTag     string `json:"image_tag"`
}

// NewBody fills a Body; times are written in UTC.
func NewBody(startedAt, finishedAt time.Time, filesWritten, apiCalls int, imageTag string) Body {
	return Body{
		StartedAt:    startedAt.UTC().Format(time.RFC3339),
		FinishedAt:   finishedAt.UTC().Format(time.RFC3339),
		FilesWritten: filesWritten,
		APICalls:     apiCalls,
		ImageTag:     imageTag,
	}
}

// ValidateName checks a --completion-marker value: lowercase letters, digits, ".", "_"
// and "-", starting with a letter or digit (it becomes one path segment).
func ValidateName(name string) error {
	if !validName.MatchString(name) {
		return fmt.Errorf("completion marker name %q: use lowercase letters, digits, '.', '_' and '-'", name)
	}
	return nil
}

// ObjectName is the marker's object name for a run that started at startedAt.
func ObjectName(name string, startedAt time.Time) (string, error) {
	if err := ValidateName(name); err != nil {
		return "", err
	}
	loc, err := time.LoadLocation(TimeZone)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s%s/%s.json", Prefix, name, startedAt.In(loc).Format("2006-01-02")), nil
}

// Result says what AfterRun did.
type Result string

const (
	Disabled Result = "disabled" // no marker name configured
	Skipped  Result = "skipped"  // dry run or failed run: never a marker
	Written  Result = "written"
	Kept     Result = "kept" // the object already existed and was left as it was
)

// AfterRun applies the marker policy at the end of a run: nothing when name is empty;
// nothing on a dry run or when runErr is set; otherwise the marker for body.StartedAt
// is created, and an existing one is logged and kept. Only a failed write is an error.
func AfterRun(ctx context.Context, store landing.Store, name string, startedAt time.Time, dryRun bool, runErr error, body Body, log *slog.Logger) (Result, error) {
	if name == "" {
		return Disabled, nil
	}
	if log == nil {
		log = slog.Default()
	}
	object, err := ObjectName(name, startedAt)
	if err != nil {
		return Skipped, err
	}
	switch {
	case runErr != nil:
		log.Info("marker_skipped", "object", object, "reason", "run_failed")
		return Skipped, nil
	case dryRun:
		log.Info("marker_skipped", "object", object, "reason", "dry_run")
		return Skipped, nil
	}
	data, err := json.Marshal(body)
	if err != nil {
		return Skipped, err
	}
	err = store.Create(ctx, object, data, ContentType, map[string]string{"marker": name})
	switch {
	case errors.Is(err, landing.ErrExists):
		log.Info("marker_exists", "object", object, "detail", "kept the existing marker")
		return Kept, nil
	case err != nil:
		return Skipped, fmt.Errorf("write marker %s: %w", object, err)
	}
	log.Info("marker_written", "object", object, "files_written", body.FilesWritten, "api_calls", body.APICalls)
	return Written, nil
}
