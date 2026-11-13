package opennebula

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/netip"
	"regexp"
	"strings"

	host "github.com/OpenNebula/one/src/oca/go/src/goca/schemas/host"
)

// CephBridgeIdentities is immutable after parsing. Its zero value allows only
// unambiguous literal registered host names, never unregistered aliases.
type CephBridgeIdentities struct {
	datastores map[int]bridgeDatastoreIdentity
}
type bridgeDatastoreIdentity struct {
	fingerprint string
	endpoints   map[string]bridgeHostIdentity
}
type bridgeHostIdentity struct {
	id   int
	name string
}

// ParseCephBridgeIdentities accepts the version1 operator configuration. It does
// not resolve DNS, fetch evidence, or turn a binding into a health attestation.
func ParseCephBridgeIdentities(raw string) (CephBridgeIdentities, error) {
	policy := CephBridgeIdentities{datastores: map[int]bridgeDatastoreIdentity{}}
	if strings.TrimSpace(raw) == "" {
		return policy, nil
	}
	if err := rejectDuplicateJSONFields(json.NewDecoder(strings.NewReader(raw))); err != nil {
		return policy, fmt.Errorf("invalid Ceph bridge identities: %w", err)
	}
	var input struct {
		Version    *int `json:"version"`
		Datastores *[]struct {
			ID          *int   `json:"datastoreID"`
			Fingerprint string `json:"configFingerprint"`
			Endpoints   []struct {
				Endpoint string `json:"endpoint"`
				ID       *int   `json:"hostID"`
				Name     string `json:"hostName"`
			} `json:"endpoints"`
		} `json:"datastores"`
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		return policy, fmt.Errorf("invalid Ceph bridge identities: %w", err)
	}
	if input.Version == nil || *input.Version != 1 || input.Datastores == nil {
		return policy, fmt.Errorf("ceph bridge identities require version1 and datastores")
	}
	digest := regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	for _, entry := range *input.Datastores {
		if entry.ID == nil || *entry.ID < 0 || !digest.MatchString(entry.Fingerprint) || len(entry.Endpoints) == 0 {
			return policy, fmt.Errorf("incomplete Ceph bridge datastore identity")
		}
		if _, exists := policy.datastores[*entry.ID]; exists {
			return policy, fmt.Errorf("duplicate bridge datastore identity %d", *entry.ID)
		}
		ds := bridgeDatastoreIdentity{fingerprint: entry.Fingerprint, endpoints: map[string]bridgeHostIdentity{}}
		for _, endpoint := range entry.Endpoints {
			if !identityToken(endpoint.Endpoint) || !identityToken(endpoint.Name) || endpoint.ID == nil || *endpoint.ID < 0 {
				return policy, fmt.Errorf("incomplete Ceph bridge endpoint identity for datastore %d", *entry.ID)
			}
			if _, exists := ds.endpoints[endpoint.Endpoint]; exists {
				return policy, fmt.Errorf("duplicate Ceph bridge endpoint %q", endpoint.Endpoint)
			}
			for _, other := range ds.endpoints {
				if (other.id == *endpoint.ID && other.name != endpoint.Name) || (other.name == endpoint.Name && other.id != *endpoint.ID) {
					return policy, fmt.Errorf("conflicting Ceph bridge host identity")
				}
			}
			ds.endpoints[endpoint.Endpoint] = bridgeHostIdentity{id: *endpoint.ID, name: endpoint.Name}
		}
		policy.datastores[*entry.ID] = ds
	}
	return policy, nil
}

// Supported endpoints are bare IP addresses without zones, or ASCII host/SSH
// aliases beginning with a letter/digit and containing only letters/digits . _ -.
func identityToken(value string) bool {
	if value == "" || len(value) > 253 {
		return false
	}
	if address, err := netip.ParseAddr(value); err == nil {
		return address.Zone() == ""
	}
	for i, r := range value {
		alphanumeric := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
		if !alphanumeric && (i == 0 || (r != '.' && r != '_' && r != '-')) {
			return false
		}
	}
	return true
}

// Walk JSON tokens before decoding so repeated object keys cannot silently replace
// reviewed fields. Only canonical field spellings are accepted; encoding/json
// alone also accepts case aliases. Required values are checked separately.
func rejectDuplicateJSONFields(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, container := token.(json.Delim)
	if container {
		switch delimiter {
		case '{':
			keys := map[string]bool{}
			for decoder.More() {
				keyToken, e := decoder.Token()
				if e != nil {
					return e
				}
				key, ok := keyToken.(string)
				if !ok {
					return fmt.Errorf("invalid object key")
				}
				switch key {
				case "version", "datastores", "datastoreID", "configFingerprint", "endpoints", "endpoint", "hostID", "hostName":
				default:
					return fmt.Errorf("unknown or noncanonical bridge identity field %q", key)
				}
				if keys[key] {
					return fmt.Errorf("duplicate JSON field %q", key)
				}
				keys[key] = true
				if e := readJSONValue(decoder); e != nil {
					return e
				}
			}
		case '[':
			for decoder.More() {
				if e := readJSONValue(decoder); e != nil {
					return e
				}
			}
		default:
			return fmt.Errorf("invalid JSON delimiter")
		}
		if _, err = decoder.Token(); err != nil {
			return err
		}
	}
	if _, err = decoder.Token(); err != io.EOF {
		if err != nil {
			return err
		}
		return fmt.Errorf("trailing JSON value")
	}
	return nil
}

func readJSONValue(decoder *json.Decoder) error {
	// Decode one raw value, retaining duplicate-field validation recursively.
	var raw json.RawMessage
	if err := decoder.Decode(&raw); err != nil {
		return err
	}
	return rejectDuplicateJSONFields(json.NewDecoder(strings.NewReader(string(raw))))
}

// bridgeTokens mirrors Bash's default IFS used by libfs.sh, preserving order.
func bridgeTokens(raw string) ([]string, error) {
	tokens := strings.FieldsFunc(raw, func(r rune) bool { return r == ' ' || r == '\t' || r == '\n' })
	if len(tokens) == 0 {
		return nil, fmt.Errorf("ceph BRIDGE_LIST is empty")
	}
	seen := map[string]bool{}
	for _, token := range tokens {
		if !identityToken(token) {
			return nil, fmt.Errorf("unsupported Ceph bridge endpoint %q", token)
		}
		if seen[token] {
			return nil, fmt.Errorf("duplicate provider BRIDGE_LIST endpoint %q", token)
		}
		seen[token] = true
	}
	return tokens, nil
}

// CephBridgeConfigFingerprint hashes encoding/json's compact encoding of this
// versioned public configuration. Capacity, provider state, timestamps, secret
// references/values, keys and configuration-file contents are deliberately absent.
func CephBridgeConfigFingerprint(ds Datastore) (string, error) {
	if ds.Ceph == nil {
		return "", fmt.Errorf("datastore %d has no Ceph configuration", ds.ID)
	}
	bridges, err := bridgeTokens(ds.Ceph.BridgeList)
	if err != nil {
		return "", err
	}
	backend := strings.ToLower(strings.TrimSpace(ds.Backend))
	if backend == "ceph" || backend == "rbd" || strings.EqualFold(ds.Type, "ceph") {
		backend = "ceph-rbd"
	}
	category := strings.ToUpper(strings.TrimSpace(ds.Category))
	if category == "" && (strings.EqualFold(ds.Type, "IMAGE") || strings.EqualFold(ds.Type, "FILE")) {
		category = strings.ToUpper(ds.Type)
	}
	payload := struct {
		Version   int      `json:"version"`
		ID        int      `json:"datastoreID"`
		Category  string   `json:"category"`
		Backend   string   `json:"backend"`
		DSMad     string   `json:"dsMad"`
		TMMad     string   `json:"tmMad"`
		DiskType  string   `json:"diskType"`
		Bridges   []string `json:"bridges"`
		Monitors  []string `json:"monitors"`
		Pool      string   `json:"pool"`
		User      string   `json:"user"`
		RBDFormat string   `json:"rbdFormat"`
		ECPool    string   `json:"ecPool"`
	}{1, ds.ID, category, backend, strings.ToLower(strings.TrimSpace(ds.DSMad)), strings.ToLower(strings.TrimSpace(ds.TMMad)), strings.ToUpper(strings.TrimSpace(ds.DiskType)), append([]string{}, bridges...), append([]string{}, normalizeMonitorList(ds.Ceph.CephHost)...), strings.TrimSpace(ds.Ceph.PoolName), strings.TrimSpace(ds.Ceph.CephUser), strings.TrimSpace(ds.Ceph.RBDFormat), strings.TrimSpace(ds.Ceph.ECPoolName)}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("sha256:%x", sha256.Sum256(encoded)), nil
}

func CephBridgeFeasible(ds Datastore, hosts []host.Host, policy CephBridgeIdentities) error {
	if ds.Ceph == nil {
		return fmt.Errorf("datastore %d has no Ceph configuration", ds.ID)
	}
	endpoints, err := bridgeTokens(ds.Ceph.BridgeList)
	if err != nil {
		return err
	}
	byID := map[int][]host.Host{}
	byName := map[string][]host.Host{}
	for _, h := range hosts {
		byID[h.ID] = append(byID[h.ID], h)
		byName[h.Name] = append(byName[h.Name], h)
	}
	binding, bound := policy.datastores[ds.ID]
	configured := map[string]bool{}
	for _, endpoint := range endpoints {
		configured[endpoint] = true
	}
	if bound {
		fingerprint, err := CephBridgeConfigFingerprint(ds)
		if err != nil {
			return err
		}
		if fingerprint != binding.fingerprint {
			return fmt.Errorf("datastore %d Ceph bridge configuration fingerprint differs: current %s", ds.ID, fingerprint)
		}
		for endpoint, expected := range binding.endpoints {
			if !configured[endpoint] {
				return fmt.Errorf("binding endpoint %q is no longer in datastore %d", endpoint, ds.ID)
			}
			if len(byID[expected.id]) != 1 || len(byName[expected.name]) != 1 || byID[expected.id][0].Name != expected.name || byName[expected.name][0].ID != expected.id {
				return fmt.Errorf("registered host identity differs or is ambiguous for endpoint %q", endpoint)
			}
		}
	}
	eligible := 0
	for _, endpoint := range endpoints {
		literal := byName[endpoint]
		expected, mapped := binding.endpoints[endpoint]
		var selected host.Host
		if len(literal) > 1 {
			return fmt.Errorf("ambiguous registered bridge name %q", endpoint)
		}
		if len(literal) == 1 {
			selected = literal[0]
			if len(byID[selected.ID]) != 1 {
				return fmt.Errorf("ambiguous registered bridge ID %d", selected.ID)
			}
			if mapped && (expected.id != selected.ID || expected.name != selected.Name) {
				return fmt.Errorf("binding conflicts with literal registered endpoint %q", endpoint)
			}

		} else {
			if !mapped {
				return fmt.Errorf("selectable Ceph bridge %q has no verified registered identity", endpoint)
			}
			selected = byID[expected.id][0]
		}
		state, err := selected.State()
		if err != nil {
			return err
		}
		if state != host.Monitored {
			return fmt.Errorf("selectable Ceph bridge %q is not currently MONITORED", endpoint)
		}
		eligible++
	}
	if eligible == 0 {
		return fmt.Errorf("no eligible selectable Ceph bridge remains")
	}
	return nil
}
