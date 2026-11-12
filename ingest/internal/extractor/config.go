package extractor

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/tmonteith/SportsAgenticLakehouse/ingest/internal/cfbd"
)

// Endpoint is a CFBD endpoint the extractor lands.
type Endpoint struct {
	Name        string // landing name (G8): the API path without "/", "/" -> "_"
	Path        string // API path
	SeasonLevel bool   // one call per season (week=00) instead of one per week
}

// Endpoints the extractor can land: the sources of silver.games (games), silver.plays
// (plays) and silver.teams (teams with their conference for the season).
var Endpoints = map[string]Endpoint{
	"games": {Name: "games", Path: "/games"},
	"plays": {Name: "plays", Path: "/plays"},
	"teams": {Name: "teams", Path: "/teams", SeasonLevel: true},
}

// DefaultEndpoints is the --endpoints default.
const DefaultEndpoints = "games,plays,teams"

// DefaultSeasonTypes is the --season-types default.
const DefaultSeasonTypes = cfbd.SeasonTypeRegular + "," + cfbd.SeasonTypePostseason

// WeekSpec selects calendar weeks.
type WeekSpec struct {
	All     bool         // every week in the season's calendar
	Recent  bool         // weeks whose dates overlap the recent window (nightly)
	Numbers map[int]bool // explicit week numbers
}

func (w WeekSpec) String() string {
	switch {
	case w.All:
		return "all"
	case w.Recent:
		return "recent"
	}
	nums := make([]int, 0, len(w.Numbers))
	for n := range w.Numbers {
		nums = append(nums, n)
	}
	sort.Ints(nums)
	parts := make([]string, len(nums))
	for i, n := range nums {
		parts[i] = strconv.Itoa(n)
	}
	return strings.Join(parts, ",")
}

// ParseSeasons parses --season: "current", a year, or a comma list of years and
// ranges ("2023,2024", "2022-2024").
func ParseSeasons(spec string, now time.Time) ([]int, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" || spec == "current" {
		return []int{cfbd.SeasonForDate(now)}, nil
	}
	seen := map[int]bool{}
	for _, tok := range strings.Split(spec, ",") {
		lo, hi, err := parseRange(strings.TrimSpace(tok))
		if err != nil {
			return nil, fmt.Errorf("--season %q: %w", spec, err)
		}
		for y := lo; y <= hi; y++ {
			if y < 1869 || y > 2100 {
				return nil, fmt.Errorf("--season %q: year %d out of range", spec, y)
			}
			seen[y] = true
		}
	}
	return sortedKeys(seen), nil
}

// ParseWeeks parses --weeks: "all", "recent", or a comma list of week numbers and
// ranges ("1-15", "1,2,5").
func ParseWeeks(spec string) (WeekSpec, error) {
	spec = strings.TrimSpace(spec)
	switch spec {
	case "all":
		return WeekSpec{All: true}, nil
	case "recent", "":
		return WeekSpec{Recent: true}, nil
	}
	nums := map[int]bool{}
	for _, tok := range strings.Split(spec, ",") {
		lo, hi, err := parseRange(strings.TrimSpace(tok))
		if err != nil {
			return WeekSpec{}, fmt.Errorf("--weeks %q: %w", spec, err)
		}
		if lo < 0 || hi > 30 {
			return WeekSpec{}, fmt.Errorf("--weeks %q: weeks must be between 0 and 30", spec)
		}
		for w := lo; w <= hi; w++ {
			nums[w] = true
		}
	}
	return WeekSpec{Numbers: nums}, nil
}

// ParseEndpoints parses --endpoints, a comma list of names from Endpoints.
func ParseEndpoints(spec string) ([]Endpoint, error) {
	var out []Endpoint
	seen := map[string]bool{}
	for _, tok := range strings.Split(spec, ",") {
		name := strings.TrimSpace(tok)
		if name == "" || seen[name] {
			continue
		}
		ep, ok := Endpoints[name]
		if !ok {
			return nil, fmt.Errorf("--endpoints: unknown endpoint %q (known: games, plays, teams)", name)
		}
		seen[name] = true
		out = append(out, ep)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("--endpoints: no endpoint selected")
	}
	return out, nil
}

// ParseSeasonTypes parses --season-types, a comma list of regular and postseason.
func ParseSeasonTypes(spec string) ([]string, error) {
	var out []string
	seen := map[string]bool{}
	for _, tok := range strings.Split(spec, ",") {
		st := strings.TrimSpace(tok)
		if st == "" || seen[st] {
			continue
		}
		if st != cfbd.SeasonTypeRegular && st != cfbd.SeasonTypePostseason {
			return nil, fmt.Errorf("--season-types: unknown season type %q (known: regular, postseason)", st)
		}
		seen[st] = true
		out = append(out, st)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("--season-types: no season type selected")
	}
	return out, nil
}

func parseRange(tok string) (int, int, error) {
	if lo, hi, ok := strings.Cut(tok, "-"); ok {
		a, err1 := strconv.Atoi(strings.TrimSpace(lo))
		b, err2 := strconv.Atoi(strings.TrimSpace(hi))
		if err1 != nil || err2 != nil || a > b {
			return 0, 0, fmt.Errorf("bad range %q", tok)
		}
		return a, b, nil
	}
	n, err := strconv.Atoi(tok)
	if err != nil {
		return 0, 0, fmt.Errorf("bad number %q", tok)
	}
	return n, n, nil
}

func sortedKeys(m map[int]bool) []int {
	out := make([]int, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Ints(out)
	return out
}
