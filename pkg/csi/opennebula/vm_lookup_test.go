package opennebula

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveNodeVMIDFromOnePoolObservation(t *testing.T) {
	for _, tc := range []struct {
		name, rows, path string
		id               int
		wantErr          error
	}{
		{"exact-first", "<VM><ID>17</ID><NAME>node-test</NAME></VM><VM><ID>18</ID><NAME>NODE-TEST</NAME></VM>", VMResolutionByName, 17, nil},
		{"unique-case-folded", "<VM><ID>17</ID><NAME>NODE-TEST</NAME></VM>", VMResolutionPoolExactMatch, 17, nil},
		{"missing", "<VM><ID>18</ID><NAME>other</NAME></VM>", VMResolutionNotFound, -1, &VMNotFoundError{}},
		{"ambiguous", "<VM><ID>17</ID><NAME>NODE-TEST</NAME></VM><VM><ID>19</ID><NAME>Node-Test</NAME></VM>", VMResolutionDuplicateMatch, -1, &VMDuplicateNameError{}},
		{"malformed-selected", "<VM><ID>17</ID><NAME>node-test</NAME><NAME>other</NAME></VM>", VMResolutionNotFound, -1, ErrConflictingAttachmentEvidence},
		{"malformed-unrelated", "<VM><ID>17</ID><NAME>node-test</NAME></VM><VM><ID>19</ID><NAME>other</NAME><LCM_STATE>bad</LCM_STATE></VM>", VMResolutionByName, 17, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			p := lifecycleFixture(t, func(method string) (bool, string, int) {
				require.Equal(t, "one.vmpool.info", method)
				calls.Add(1)
				return true, "<VM_POOL>" + tc.rows + "</VM_POOL>", 0
			})
			id, path, err := ResolveNodeVMID(context.Background(), p.ctrl, "node-test")
			switch tc.wantErr.(type) {
			case *VMNotFoundError:
				require.ErrorAs(t, err, new(*VMNotFoundError))
			case *VMDuplicateNameError:
				require.ErrorAs(t, err, new(*VMDuplicateNameError))
			default:
				require.ErrorIs(t, err, tc.wantErr)
			}
			require.Equal(t, tc.id, id)
			require.Equal(t, tc.path, path)
			require.EqualValues(t, 1, calls.Load())
		})
	}
}
