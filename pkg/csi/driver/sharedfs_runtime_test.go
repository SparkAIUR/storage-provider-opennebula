package driver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSharedFilesystemProbeTranslatesDisconnectedStatDiagnostics(t *testing.T) {
	const path = "/var/lib/kubelet/plugins/kubernetes.io/csi/csi.opennebula.io/test/globalmount"
	exitErr := errors.New("exit status 1")
	for _, test := range []struct {
		name         string
		path         string
		diagnostic   string
		err          error
		disconnected bool
	}{
		{name: "musl-statx", path: path, diagnostic: "stat: cannot statx '" + path + "': Socket not connected\n", disconnected: true},
		{name: "glibc-statx", path: path, diagnostic: "stat: cannot statx '" + path + "': Transport endpoint is not connected\n", disconnected: true},
		{name: "glibc-stat", path: path, diagnostic: "stat: cannot stat '" + path + "': Transport endpoint is not connected\n", disconnected: true},
		{name: "typed-enotconn", path: path, err: &os.PathError{Op: "stat", Path: path, Err: syscall.ENOTCONN}, disconnected: true},
		{name: "unknown-io", path: path, diagnostic: "stat: cannot statx '" + path + "': Input/output error\n"},
		{name: "permission", path: path, diagnostic: "stat: cannot statx '" + path + "': Permission denied\n"},
		{name: "misleading-path", path: path + "/Socket not connected", diagnostic: "stat: cannot statx '" + path + "/Socket not connected': Permission denied\n"},
		{name: "misleading-glibc-path", path: path + "/Transport endpoint is not connected", diagnostic: "stat: cannot statx '" + path + "/Transport endpoint is not connected': Permission denied\n"},
		{name: "wrong-path", path: path, diagnostic: "stat: cannot statx '/other': Socket not connected\n"},
		{name: "canceled", path: path, diagnostic: "stat: cannot statx '" + path + "': Socket not connected\n", err: context.Canceled},
		{name: "deadline", path: path, diagnostic: "stat: cannot statx '" + path + "': Socket not connected\n", err: context.DeadlineExceeded},
	} {
		t.Run(test.name, func(t *testing.T) {
			commandErr := test.err
			if commandErr == nil {
				commandErr = fmt.Errorf("stat failed: %w: %s", exitErr, test.diagnostic)
			}
			runtime := newSharedFilesystemRuntime(nil)
			runtime.run = func(_ context.Context, name string, args ...string) ([]byte, error) {
				require.Equal(t, "stat", name)
				require.Equal(t, []string{"-L", "--format=%F", "--", test.path}, args)
				return []byte(test.diagnostic), commandErr
			}
			err := runtime.probe(context.Background(), test.path)
			require.Error(t, err)
			require.ErrorIs(t, err, commandErr, "retain the subprocess failure for diagnosis")
			if test.disconnected {
				require.ErrorIs(t, err, syscall.ENOTCONN)
				var pathErr *os.PathError
				require.ErrorAs(t, err, &pathErr)
				require.Equal(t, "stat", pathErr.Op)
				require.Equal(t, test.path, pathErr.Path)
				require.True(t, isDisconnectedSharedFilesystemError(err))
			} else {
				require.Equal(t, commandErr, err, "unknown or canceled failures must remain unchanged")
				require.False(t, errors.Is(err, syscall.ENOTCONN))
				require.False(t, isDisconnectedSharedFilesystemError(err))
			}
		})
	}
}
