package driver

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode"
)

var errDuplicateAuthorityKey = errors.New("duplicate JSON field in authority record")

func unmarshalAuthorityJSON(raw string, target any) error {
	if err := requireUniqueAuthorityKeys(raw); err != nil {
		return err
	}
	return json.Unmarshal([]byte(raw), target)
}

func unmarshalAuthorityRoute(raw string, target any, fields ...string) error {
	decoder := json.NewDecoder(strings.NewReader(raw))
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	duplicates := false
	if token == json.Delim('{') {
		seen := make(map[string]any)
		for decoder.More() {
			token, err := decoder.Token()
			if err != nil {
				return err
			}
			key := token.(string)
			var rawValue json.RawMessage
			if err := decoder.Decode(&rawValue); err != nil {
				return err
			}
			for _, field := range fields {
				if strings.EqualFold(key, field) {
					valueDecoder := json.NewDecoder(strings.NewReader(string(rawValue)))
					valueDecoder.UseNumber()
					value, err := valueDecoder.Token()
					if err != nil {
						return err
					}
					if previous, duplicate := seen[field]; duplicate {
						switch value.(type) {
						case string, json.Number:
							if previous == value {
								duplicates = true
								break
							}
							return fmt.Errorf("%w: %q", errDuplicateAuthorityKey, key)
						default:
							return fmt.Errorf("%w: %q", errDuplicateAuthorityKey, key)
						}
					}
					seen[field] = value
					break
				}
			}
		}
	}
	err = json.Unmarshal([]byte(raw), target)
	if err != nil && duplicates {
		return fmt.Errorf("%w: %v", errDuplicateAuthorityKey, err)
	}
	return err
}

func requireUniqueAuthorityKeys(raw string) error {
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	return checkAuthorityJSONValue(decoder)
}

func checkAuthorityJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	switch token {
	case json.Delim('{'):
		seen := make(map[string]struct{})
		for decoder.More() {
			token, err := decoder.Token()
			if err != nil {
				return err
			}
			key := token.(string)
			folded := strings.Map(foldAuthorityKeyRune, key)
			if _, duplicate := seen[folded]; duplicate {
				return fmt.Errorf("%w: %q", errDuplicateAuthorityKey, key)
			}
			seen[folded] = struct{}{}
			if err := checkAuthorityJSONValue(decoder); err != nil {
				return err
			}
		}
	case json.Delim('['):
		for decoder.More() {
			if err := checkAuthorityJSONValue(decoder); err != nil {
				return err
			}
		}
	default:
		return nil
	}
	_, err = decoder.Token()
	return err
}

func foldAuthorityKeyRune(r rune) rune {
	for {
		next := unicode.SimpleFold(r)
		if next <= r {
			return next
		}
		r = next
	}
}
