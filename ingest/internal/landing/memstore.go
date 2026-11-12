package landing

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// MemStore is an in-memory Store for tests and dry runs. It enforces the same
// never-overwrite rule as GCS.
type MemStore struct {
	mu      sync.Mutex
	objects map[string]memObject
	Creates int // successful Create calls
}

type memObject struct {
	data        []byte
	contentType string
	metadata    map[string]string
}

// NewMemStore returns an empty MemStore.
func NewMemStore() *MemStore { return &MemStore{objects: map[string]memObject{}} }

func (m *MemStore) List(_ context.Context, prefix string) ([]Object, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Object
	for name, o := range m.objects {
		if strings.HasPrefix(name, prefix) {
			out = append(out, Object{Name: name, Metadata: copyMap(o.metadata)})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (m *MemStore) Create(_ context.Context, name string, data []byte, contentType string, metadata map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.objects[name]; ok {
		return fmt.Errorf("%s: %w", name, ErrExists)
	}
	m.objects[name] = memObject{data: append([]byte(nil), data...), contentType: contentType, metadata: copyMap(metadata)}
	m.Creates++
	return nil
}

// Get returns an object's data, content type and metadata.
func (m *MemStore) Get(name string) (data []byte, contentType string, metadata map[string]string, ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.objects[name]
	return o.data, o.contentType, copyMap(o.metadata), ok
}

// Names returns every object name, sorted.
func (m *MemStore) Names() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	names := make([]string, 0, len(m.objects))
	for n := range m.objects {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

func copyMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
