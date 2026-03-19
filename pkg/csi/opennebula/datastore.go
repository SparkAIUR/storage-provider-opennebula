package opennebula

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	datastoreSchema "github.com/OpenNebula/one/src/oca/go/src/goca/schemas/datastore"
)

const mib = 1024 * 1024

type DatastoreSelectionPolicy string

const (
	DatastoreSelectionPolicyLeastUsed DatastoreSelectionPolicy = "least-used"
	DatastoreSelectionPolicyOrdered   DatastoreSelectionPolicy = "ordered"
)

type DatastoreSelectionConfig struct {
	Identifiers  []string
	Policy       DatastoreSelectionPolicy
	AllowedTypes []string
}

type Datastore struct {
	ID        int
	Name      string
	Type      string
	DSMad     string
	TMMad     string
	FreeBytes int64
	Enabled   bool
}

type VolumeCreateResult struct {
	Datastore Datastore
}

type DatastoreSelector interface {
	Sort([]Datastore) []Datastore
}

type datastoreConfigError struct {
	message string
}

func (e *datastoreConfigError) Error() string {
	return e.message
}

type datastoreCapacityError struct {
	message string
}

func (e *datastoreCapacityError) Error() string {
	return e.message
}

func IsDatastoreConfigError(err error) bool {
	_, ok := err.(*datastoreConfigError)
	return ok
}

func IsDatastoreCapacityError(err error) bool {
	_, ok := err.(*datastoreCapacityError)
	return ok
}

func NormalizeDatastoreSelectionPolicy(policy string) DatastoreSelectionPolicy {
	switch strings.ToLower(strings.TrimSpace(policy)) {
	case "", string(DatastoreSelectionPolicyLeastUsed):
		return DatastoreSelectionPolicyLeastUsed
	case string(DatastoreSelectionPolicyOrdered):
		return DatastoreSelectionPolicyOrdered
	default:
		return DatastoreSelectionPolicy(strings.ToLower(strings.TrimSpace(policy)))
	}
}

func NewDatastoreSelector(policy DatastoreSelectionPolicy) (DatastoreSelector, error) {
	switch NormalizeDatastoreSelectionPolicy(string(policy)) {
	case DatastoreSelectionPolicyLeastUsed:
		return leastUsedDatastoreSelector{}, nil
	case DatastoreSelectionPolicyOrdered:
		return orderedDatastoreSelector{}, nil
	default:
		return nil, &datastoreConfigError{message: fmt.Sprintf("unsupported datastore selection policy %q", policy)}
	}
}

func ResolveDatastores(pool []datastoreSchema.Datastore, selection DatastoreSelectionConfig) ([]Datastore, error) {
	if len(selection.Identifiers) == 0 {
		return nil, &datastoreConfigError{message: "no datastores configured for provisioning"}
	}

	allowedTypes := normalizeAllowedDatastoreTypes(selection.AllowedTypes)
	poolByID := make(map[int]datastoreSchema.Datastore, len(pool))
	var defaultDatastore *datastoreSchema.Datastore
	for _, candidate := range pool {
		poolByID[candidate.ID] = candidate
		if candidate.Name == "default" {
			candidateCopy := candidate
			defaultDatastore = &candidateCopy
		}
	}

	resolved := make([]Datastore, 0, len(selection.Identifiers))
	seen := make(map[int]struct{}, len(selection.Identifiers))
	for _, identifier := range selection.Identifiers {
		candidate, err := resolveDatastoreIdentifier(identifier, poolByID, defaultDatastore)
		if err != nil {
			return nil, err
		}

		normalized := datastoreFromSchema(candidate)
		if !normalized.Enabled {
			return nil, &datastoreConfigError{message: fmt.Sprintf("datastore %q is not enabled", identifier)}
		}

		if _, ok := allowedTypes[normalized.Type]; !ok {
			return nil, &datastoreConfigError{
				message: fmt.Sprintf("datastore %d resolved as type %q, which is not allowed", normalized.ID, normalized.Type),
			}
		}

		if _, ok := seen[normalized.ID]; ok {
			continue
		}
		seen[normalized.ID] = struct{}{}
		resolved = append(resolved, normalized)
	}

	if len(resolved) == 0 {
		return nil, &datastoreConfigError{message: "no eligible datastores found for provisioning"}
	}

	return resolved, nil
}

func OrderDatastores(candidates []Datastore, policy DatastoreSelectionPolicy) ([]Datastore, error) {
	selector, err := NewDatastoreSelector(policy)
	if err != nil {
		return nil, err
	}

	return selector.Sort(candidates), nil
}

func SumDatastoreCapacity(candidates []Datastore) int64 {
	var total int64
	for _, candidate := range candidates {
		total += candidate.FreeBytes
	}

	return total
}

func normalizeAllowedDatastoreTypes(values []string) map[string]struct{} {
	if len(values) == 0 {
		values = []string{"local"}
	}

	normalized := make(map[string]struct{}, len(values))
	for _, value := range values {
		normalized[normalizeAllowedDatastoreType(value)] = struct{}{}
	}

	return normalized
}

func resolveDatastoreIdentifier(identifier string, poolByID map[int]datastoreSchema.Datastore, defaultDatastore *datastoreSchema.Datastore) (datastoreSchema.Datastore, error) {
	trimmed := strings.TrimSpace(identifier)
	if trimmed == "" {
		return datastoreSchema.Datastore{}, &datastoreConfigError{message: "datastore identifier cannot be empty"}
	}

	if strings.EqualFold(trimmed, "default") {
		if defaultDatastore == nil {
			return datastoreSchema.Datastore{}, &datastoreConfigError{message: "default datastore alias was requested but no datastore named \"default\" exists"}
		}

		return *defaultDatastore, nil
	}

	id, err := strconv.Atoi(trimmed)
	if err != nil {
		return datastoreSchema.Datastore{}, &datastoreConfigError{
			message: fmt.Sprintf("unsupported datastore identifier %q: use datastore IDs or the default alias", trimmed),
		}
	}

	candidate, ok := poolByID[id]
	if !ok {
		return datastoreSchema.Datastore{}, &datastoreConfigError{message: fmt.Sprintf("datastore %d was not found", id)}
	}

	return candidate, nil
}

func datastoreFromSchema(source datastoreSchema.Datastore) Datastore {
	enabled := true
	if state, err := source.State(); err == nil {
		enabled = state == datastoreSchema.Ready
	}

	return Datastore{
		ID:        source.ID,
		Name:      source.Name,
		Type:      normalizeAllowedDatastoreType(inferDatastoreType(source)),
		DSMad:     strings.ToLower(strings.TrimSpace(source.DSMad)),
		TMMad:     strings.ToLower(strings.TrimSpace(source.TMMad)),
		FreeBytes: int64(source.FreeMB) * mib,
		Enabled:   enabled,
	}
}

func inferDatastoreType(source datastoreSchema.Datastore) string {
	values := []string{
		strings.ToLower(strings.TrimSpace(source.TMMad)),
		strings.ToLower(strings.TrimSpace(source.DSMad)),
		strings.ToLower(strings.TrimSpace(source.Type)),
	}

	for _, value := range values {
		switch value {
		case "local", "ceph", "nfs", "nas":
			return value
		case "fs":
			if strings.EqualFold(source.TMMad, "local") {
				return "local"
			}
		}
	}

	for _, value := range values {
		if value != "" {
			return value
		}
	}

	return "unknown"
}

func normalizeAllowedDatastoreType(value string) string {
	normalized := strings.ToLower(strings.TrimSpace(value))
	switch normalized {
	case "", "nas":
		return "nfs"
	case "fs":
		return "local"
	default:
		return normalized
	}
}

type leastUsedDatastoreSelector struct{}

func (leastUsedDatastoreSelector) Sort(candidates []Datastore) []Datastore {
	sorted := append([]Datastore(nil), candidates...)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].FreeBytes == sorted[j].FreeBytes {
			return false
		}
		return sorted[i].FreeBytes > sorted[j].FreeBytes
	})
	return sorted
}

type orderedDatastoreSelector struct{}

func (orderedDatastoreSelector) Sort(candidates []Datastore) []Datastore {
	return append([]Datastore(nil), candidates...)
}
