package driver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"k8s.io/klog/v2"
	mount "k8s.io/mount-utils"
)

// Reserve one second of the 60-second budget for reaping cancelled children.
const sharedFilesystemAttemptTimeout = 59 * time.Second

// The runtime keeps potentially blocked filesystem syscalls out of the node
// server. Tests supply an in-memory mount table and executable fake operations.
type sharedFilesystemRuntime struct {
	run       func(context.Context, string, ...string) ([]byte, error)
	probe     func(context.Context, string) error
	bind      func(context.Context, string, string, []string) error
	unmount   func(context.Context, string) error
	mkdir     func(context.Context, string) error
	rmdir     func(context.Context, string) error
	mountInfo func() ([]mount.MountInfo, error)
	fuse      func(context.Context, sharedFilesystemSession, []string) error
}

func newSharedFilesystemRuntime(ns *NodeServer) *sharedFilesystemRuntime {
	r := &sharedFilesystemRuntime{run: runSharedFilesystemCommand}
	r.mountInfo = func() ([]mount.MountInfo, error) { return mount.ParseMountInfo("/proc/self/mountinfo") }
	r.probe = func(ctx context.Context, path string) error {
		output, err := r.run(ctx, "stat", "-L", "--format=%F", "--", path)
		if err == nil || errors.Is(err, syscall.ENOTCONN) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		// stat exposes errno only as a diagnostic. Alpine/musl and glibc use
		// different ENOTCONN messages. Match the complete diagnostic for this
		// exact path; a message embedded in a pathname cannot authorize recovery.
		diagnostic := strings.TrimSpace(string(output))
		for _, operation := range []string{"cannot statx", "cannot stat"} {
			prefix := fmt.Sprintf("stat: %s '%s': ", operation, path)
			if diagnostic == prefix+"Socket not connected" || diagnostic == prefix+"Transport endpoint is not connected" {
				return &os.PathError{Op: "stat", Path: path, Err: errors.Join(syscall.ENOTCONN, err)}
			}
		}
		return err
	}
	r.bind = func(ctx context.Context, stage, target string, options []string) error {
		_, err := r.run(ctx, "mount", "-o", strings.Join(options, ","), "--", stage, target)
		return err
	}
	r.unmount = func(ctx context.Context, path string) error {
		_, err := r.run(ctx, "umount", "--", path)
		return err
	}
	r.mkdir = func(ctx context.Context, path string) error {
		_, err := r.run(ctx, "mkdir", "-p", "-m", "0750", "--", path)
		return err
	}
	r.rmdir = func(ctx context.Context, path string) error {
		_, err := r.run(ctx, "rmdir", "--", path)
		return err
	}
	r.fuse = ns.startSharedFilesystemFuse
	return r
}

func sharedFilesystemCommand(ctx context.Context, name string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, name, args...)
	if name == "stat" {
		// Keep the stat diagnostic used for errno translation locale-independent.
		cmd.Env = append(os.Environ(), "LC_ALL=C")
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	cmd.WaitDelay = time.Second
	return cmd
}

func runSharedFilesystemCommand(ctx context.Context, name string, args ...string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cmd := sharedFilesystemCommand(ctx, name, args...)
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	done := make(chan struct{})
	var waitErr error
	go func() { waitErr = cmd.Wait(); close(done) }()
	if err := awaitSharedFilesystemExit(ctx, done, func() { _ = cmd.Cancel() }); err != nil {
		// A quarantined child may still own its output buffer; never read it.
		return nil, err
	}
	if waitErr != nil {
		return output.Bytes(), fmt.Errorf("%s failed: %w: %s", name, waitErr, strings.TrimSpace(output.String()))
	}
	return output.Bytes(), nil
}

// A foreground client cannot escape cancellation by daemonizing. Once mounted,
// its lifetime belongs to the volume, not to the RPC which established it.
// Wait always reaps it and records an exit before queuing recovery.
func (ns *NodeServer) startSharedFilesystemFuse(ctx context.Context, session sharedFilesystemSession, args []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	cmd := exec.Command("ceph-fuse", append([]string{"-f"}, args...)...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	done := make(chan struct{})
	var exitErr error
	go func() {
		exitErr = cmd.Wait()
		klog.InfoS("CephFS client exited", "volumeID", session.VolumeID, "pid", cmd.Process.Pid, "error", exitErr)
		close(done)
		if exitErr != nil && ns.sharedFilesystemRecovery != nil {
			ns.sharedFilesystemRecovery.enqueue(session.VolumeID, "fuse_exit")
		}
	}()
	success := false
	defer func() {
		if !success {
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			_ = awaitSharedFilesystemExit(cancelled, done, func() { _ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) })
		}
	}()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			return fmt.Errorf("CephFS client exited before mounting: %v", exitErr)
		case <-ticker.C:
			_, mounted, err := ns.mountPointForPath(session.StagingTargetPath)
			if err != nil {
				return err
			}
			if mounted && ns.sharedFS.probe(ctx, session.StagingTargetPath) == nil {
				if err := ctx.Err(); err != nil {
					return err
				}
				select {
				case <-done:
					return fmt.Errorf("CephFS client exited during mount readiness: %v", exitErr)
				default:
				}
				success = true
				return nil
			}
		}
	}
}

// Never descend into a mountpoint. An unsuccessful unmount retains all session
// state, and a fresh mount table must prove absence before directory removal.
func (ns *NodeServer) cleanupSharedFilesystemPath(ctx context.Context, path string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	points, err := ns.mounter.List()
	if err != nil {
		return err
	}
	mounted := false
	mountCount := 0
	for _, point := range points {
		if strings.HasPrefix(point.Path, strings.TrimRight(path, "/")+"/") {
			return fmt.Errorf("refusing cleanup of %s with nested mount %s", path, point.Path)
		}
		if point.Path == path {
			mountCount++
			if mountCount > 1 {
				return fmt.Errorf("refusing cleanup of stacked mounts at %s", path)
			}
			if point.Type != "ceph" && point.Type != "fuse.ceph-fuse" {
				return fmt.Errorf("refusing cleanup of non-CephFS mount %s", path)
			}
			mounted = true
		}
	}
	if mounted {
		if err := ns.sharedFS.unmount(ctx, path); err != nil {
			return err
		}
	}
	_, mounted, err = ns.mountPointForPath(path)
	if err != nil {
		return err
	}
	if mounted {
		return fmt.Errorf("mount remains at %s after unmount", path)
	}
	// A missing directory is already clean. Check only after proving no mount.
	if _, err := os.Lstat(path); os.IsNotExist(err) {
		return nil
	}
	return ns.sharedFS.rmdir(ctx, path)
}
