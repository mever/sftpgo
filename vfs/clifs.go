// +build clifs

package vfs

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/drakkan/sftpgo/logger"
	"github.com/drakkan/sftpgo/metrics"
	"github.com/eikenb/pipeat"
	"github.com/pkg/errors"
	"github.com/pkg/sftp"
)

const (
	// cliFsName is the name for the Command Line Interface Fs implementation
	cliFsName = "clifs"
)

type CliFsConfig struct {

	// BinPath is the path to a binary that will receive filesystem operations.
	BinPath string

	// ExtraCommandArgs is a JSON array of strings of extra command-line arguments for the CLI program that are added before the file system operation name.
	ExtraCommandArgs string
}

func (c *CliFsConfig) isEqual(other *CliFsConfig) bool {
	return c.BinPath == other.BinPath && c.ExtraCommandArgs == other.ExtraCommandArgs
}

// Validate returns an error if the configuration is not valid
func (c *CliFsConfig) Validate() error {
	return nil
}

type CliFs struct {
	name         string
	connectionID string
	localTempDir string
	// if not empty this fs is mouted as virtual folder in the specified path
	mountPath string
	config    *CliFsConfig
}

func NewCLIFs(connectionID string, mountPath, localTempDir string, config CliFsConfig) (*CliFs, error) {
	return &CliFs{
		name:         cliFsName,
		connectionID: connectionID,
		mountPath:    mountPath,
		localTempDir: localTempDir,
		config:       &config,
	}, nil
}

// Name returns the name for the Fs implementation
func (fs *CliFs) Name() string {
	return fs.name
}

// ConnectionID returns the SSH connection ID associated to this Fs implementation
func (fs *CliFs) ConnectionID() string {
	return fs.connectionID
}

// Stat returns a FileInfo describing the named file
func (fs *CliFs) Stat(name string) (os.FileInfo, error) {
	m, er := fs.callMustMap("stat", name)
	if er != nil {
		return nil, errors.Wrap(er, "calling stat command failed")
	}

	if m == nil {
		return nil, newNotExistsError("file or directory does not exist: " + name)
	}

	return newFileInfoFromMap(m)
}

// Lstat returns a FileInfo describing the named file, or nil if there is no such file.
func (fs *CliFs) Lstat(name string) (os.FileInfo, error) {
	m, er := fs.callCanMap("lstat", name)
	if er != nil {
		return nil, errors.Wrap(er, "calling lstat command failed")
	}

	if m == nil {
		return nil, newNotExistsError("file or directory does not exist: " + name)
	}

	return newFileInfoFromMap(m)
}

// Open opens the named file for reading
func (fs *CliFs) Open(name string, offset int64) (File, *pipeat.PipeReaderAt, func(), error) {
	var args []string
	if er := json.Unmarshal([]byte(fs.config.ExtraCommandArgs), &args); er != nil {
		return nil, nil, nil, errors.Wrap(er, "failed to decode extra command args")
	}
	a := append(args, "open")
	a = append(a, name)
	a = append(a, strconv.Itoa(int(offset)))

	r, w, er := pipeat.PipeInDir(fs.localTempDir)
	if er != nil {
		return nil, nil, nil, errors.Wrap(er, "failed to create pipeat")
	}

	ctx, cancelFn := context.WithCancel(context.Background())

	cmd := exec.CommandContext(ctx, fs.config.BinPath, a...)
	cmd.Stdout = w
	if er2 := cmd.Start(); er2 != nil {
		cancelFn()
		_ = w.Close()
		return nil, nil, nil, errors.Wrap(er2, "failed to start command process")
	}

	go func() {
		defer func() {
			cancelFn()
		}()
		err := errors.Wrap(cmd.Wait(), "wait failure")
		n := w.GetWrittenBytes()
		_ = w.CloseWithError(errors.Wrap(err, "failed to copy"))
		fsLog(fs, logger.LevelDebug, "download completed, path: %#v size: %v, err: %v", name, n, err)
		metrics.TransferCompleted(n, 0, 1, err)
	}()

	return nil, r, cancelFn, nil
}

// Create creates or opens the named file for writing
func (fs *CliFs) Create(name string, flag int) (File, *PipeWriter, func(), error) {
	var args []string
	if er := json.Unmarshal([]byte(fs.config.ExtraCommandArgs), &args); er != nil {
		return nil, nil, nil, errors.Wrap(er, "failed to decode extra command args")
	}
	a := append(args, "create")
	a = append(a, name)
	a = append(a, strconv.Itoa(flag))

	r, w, err := pipeat.PipeInDir(fs.localTempDir)
	if err != nil {
		return nil, nil, nil, err
	}
	p := NewPipeWriter(w)
	ctx, cancelFn := context.WithCancel(context.Background())

	cmd := exec.CommandContext(ctx, fs.config.BinPath, a...)

	stdErrBuf := bytes.Buffer{}
	cmd.Stderr = &stdErrBuf
	cmd.Stdin = r
	if er := cmd.Start(); er != nil {
		cancelFn()
		_ = p.Close()
		_ = r.Close()
		return nil, nil, nil, errors.Wrap(er, "failed to start command")
	}
	go func() {
		defer cancelFn()
		var er3 error
		errWait := cmd.Wait()
		if errWait != nil {
			er3 = getErrorFromStatus(stdErrBuf.Bytes())
		}
		if er3 == nil {
			er3 = errors.Wrap(errWait, "wait failure")
		}
		n := r.GetReadedBytes()
		_ = r.CloseWithError(er3)
		p.Done(er3)

		fsLog(fs, logger.LevelDebug, "upload completed, path: %#v, readed bytes: %v, err: %v", name, n, er3)
		metrics.GCSTransferCompleted(n, 0, er3)
	}()
	return nil, p, cancelFn, nil
}

// MkdirAll does nothing, we don't have folder
func (*CliFs) MkdirAll(name string, uid int, gid int) error {
	return nil
}

// Symlink creates source as a symbolic link to target.
func (*CliFs) Symlink(source, target string) error {
	return ErrVfsUnsupported
}

// Readlink returns the destination of the named symbolic link
func (*CliFs) Readlink(name string) (string, error) {
	return "", ErrVfsUnsupported
}

// Chown changes the numeric uid and gid of the named file.
func (*CliFs) Chown(name string, uid int, gid int) error {
	return ErrVfsUnsupported
}

// Chmod changes the mode of the named file to mode.
func (*CliFs) Chmod(name string, mode os.FileMode) error {
	return ErrVfsUnsupported
}

// Chtimes changes the access and modification times of the named file.
func (*CliFs) Chtimes(name string, atime, mtime time.Time) error {
	return ErrVfsUnsupported
}

// Truncate changes the size of the named file.
// Truncate by path is not supported, while truncating an opened
// file is handled inside base transfer
func (*CliFs) Truncate(name string, size int64) error {
	return ErrVfsUnsupported
}

// Rename renames (moves) source to target
func (fs *CliFs) Rename(source, target string) error {
	_, er := fs.call("rename", source, target)
	return er
}

// Remove removes the named file or (empty) directory.
func (fs *CliFs) Remove(name string, isDir bool) error {
	isDirStr := "0"
	if isDir {
		isDirStr = "1"
	}
	_, er := fs.call("remove", name, isDirStr)
	return er
}

// Mkdir creates a new directory with the specified name and default permissions
func (*CliFs) Mkdir(name string) error {
	return ErrVfsUnsupported
}

// ReadDir reads the directory named by dirname and returns
// a list of directory entries.
func (fs *CliFs) ReadDir(dirname string) ([]os.FileInfo, error) {
	m, er := fs.callMustMap("readDir", dirname)
	if er != nil {
		return nil, er
	}

	if res, has := m["list"]; has {
		if list, ok := res.([]interface{}); ok {
			infos := make([]os.FileInfo, len(list))
			for i, item := range list {
				if lMap, ok := item.(map[string]interface{}); ok {
					infos[i], er = newFileInfoFromMap(lMap)
					if er != nil {
						return nil, errors.Wrapf(er, "failed to cast item at #%d to FileInfo struct", i+1)
					}
				} else {
					return nil, errors.Errorf("failed to cast item at #%d to map", i+1)
				}
			}
			return infos, nil

		} else {
			return nil, errors.New("list must be an array")
		}
	} else {
		return nil, errors.New("returned JSON must contain 'list' as an object key")
	}
}

// IsUploadResumeSupported returns true if resuming uploads is supported
func (*CliFs) IsUploadResumeSupported() bool {
	return false
}

// IsAtomicUploadSupported returns true if atomic upload is supported
func (*CliFs) IsAtomicUploadSupported() bool {
	return false
}

// IsNotExist returns a boolean indicating whether the error is known to
// report that a file or directory does not exist
func (*CliFs) IsNotExist(err error) bool {
	_, ok := err.(notExistsError)
	return ok
}

// IsPermission returns a boolean indicating whether the error is known to
// report that permission is denied.
func (*CliFs) IsPermission(err error) bool {
	_, ok := err.(permissionDeniedError)
	return ok
}

// IsNotSupported returns true if the error indicate an unsupported operation
func (*CliFs) IsNotSupported(err error) bool {
	if err == nil {
		return false
	}
	return err == ErrVfsUnsupported
}

// CheckRootPath creates the root directory if it does not exists
func (fs *CliFs) CheckRootPath(username string, uid int, gid int) bool {
	// we need a local directory for temporary files
	osFs := NewOsFs(fs.ConnectionID(), fs.localTempDir, "")
	return osFs.CheckRootPath(username, uid, gid)
}

// ScanRootDirContents returns the number of files contained in the root
// directory and their size
func (fs *CliFs) ScanRootDirContents() (int, int64, error) {
	// FIXME: Just report empty for now.
	return 0, 0, nil
}

// GetAtomicUploadPath returns the path to use for an atomic upload
func (*CliFs) GetAtomicUploadPath(name string) string {
	return ""
}

// GetRelativePath returns the path for a file relative to the user's home dir.
// This is the path as seen by SFTPGo users
func (fs *CliFs) GetRelativePath(name string) string {
	rel := path.Clean(name)
	if rel == "." {
		rel = ""
	}
	if !path.IsAbs(rel) {
		rel = "/" + rel
	}
	if fs.mountPath != "" {
		rel = path.Join(fs.mountPath, rel)
	}
	return rel
}

// Walk walks the file tree rooted at root, calling walkFn for each file or
// directory in the tree, including root
func (*CliFs) Walk(root string, walkFn filepath.WalkFunc) error {
	// FIXME:
	return ErrVfsUnsupported
}

// Join joins any number of path elements into a single path
func (*CliFs) Join(elem ...string) string {
	return filepath.Join(elem...)
}

// ResolvePath returns the matching filesystem path for the specified sftp path
func (fs *CliFs) ResolvePath(virtualPath string) (string, error) {
	if fs.mountPath != "" {
		virtualPath = strings.TrimPrefix(virtualPath, fs.mountPath)
	}
	if !path.IsAbs(virtualPath) {
		virtualPath = path.Clean("/" + virtualPath)
	}
	return strings.TrimPrefix(virtualPath, "/"), nil
}

// GetDirSize returns the number of files and the size for a folder
// including any subfolders
func (fs *CliFs) GetDirSize(dirname string) (int, int64, error) {
	return 0, 0, ErrVfsUnsupported
}

// HasVirtualFolders returns true if folders are emulated
func (*CliFs) HasVirtualFolders() bool {
	return false
}

// GetMimeType returns the content type
func (fs *CliFs) GetMimeType(name string) (string, error) {
	m, er := fs.callMustMap("getMimeType", name)
	if er != nil {
		return "", errors.Wrap(er, "calling command failed")
	}
	if res, has := m["result"]; has {
		if t, ok := res.(string); ok {
			return t, nil
		} else {
			return "", errors.New("result must be a string")
		}
	} else {
		return "", errors.New("returned JSON must contain 'result' as an object key")
	}
}

// Close closes the fs
func (*CliFs) Close() error {
	return nil
}

// GetAvailableDiskSize return the available size for the specified path
func (*CliFs) GetAvailableDiskSize(dirName string) (*sftp.StatVFS, error) {
	return nil, ErrStorageSizeUnavailable
}

// callMustMap executes a call on the fs and always returns a map on success.
func (fs CliFs) callMustMap(name string, args ...string) (map[string]interface{}, error) {
	b, err := fs.call(name, args...)
	if err != nil {
		return nil, err
	}

	return toMap(b)
}

// callCanMap executes a call on the fs and can optionally return a map or nil.
func (fs CliFs) callCanMap(name string, args ...string) (map[string]interface{}, error) {
	b, err := fs.call(name, args...)
	if err != nil {
		return nil, err
	}

	if len(b) == 0 {
		return nil, nil
	}

	return toMap(b)
}

func toMap(b []byte) (map[string]interface{}, error) {
	m := make(map[string]interface{})
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, errors.Wrap(err, "failed to decode JSON")
	}

	return m, nil
}

func (fs CliFs) call(name string, args ...string) ([]byte, error) {
	var flags []string
	if er := json.Unmarshal([]byte(fs.config.ExtraCommandArgs), &flags); er != nil {
		return nil, errors.Wrap(er, "failed to decode extra command flags")
	}

	a := append(flags, name)
	a = append(a, args...)
	cmd := exec.Command(fs.config.BinPath, a...)

	stdErrBuf := bytes.Buffer{}
	var stdoutBuf bytes.Buffer
	cmd.Stderr = &stdErrBuf
	cmd.Stdout = &stdoutBuf

	erRun := cmd.Run()
	if erRun != nil {
		if s := getErrorFromStatus(stdErrBuf.Bytes()); s != nil {
			return nil, s
		}
		return nil, errors.Wrap(erRun, "failed to run command: " + fs.config.BinPath + " " + strings.Join(a, " "))
	}

	return stdoutBuf.Bytes(), nil
}

func newFileInfoFromMap(m map[string]interface{}) (fi *FileInfo, er error) {
	defer func() {
		r := recover()
		if r != nil {
			message := "failed to cast file info map to struct"
			if e, ok := r.(error); ok {
				er = errors.Wrap(e, message)
			} else {
				er = errors.New(message)
			}
		}
	}()
	t, _ := time.Parse(time.RFC3339, m["modTime"].(string))
	fi = NewFileInfo(m["name"].(string), m["isDirectory"].(bool),
		int64(m["sizeInBytes"].(float64)), t, m["fullName"].(bool))

	return
}

const (
	StatusTypeSuccess int = iota
	StatusTypeWarning
	StatusTypeError
	StatusTypeUnknown
)

const (
	StatusCodeFeedback int = iota
	StatusCodeNotExists
	StatusCodePermissionDenied
)

type Status struct {
	Type    int    `json:"type"`
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func getErrorFromStatus(b []byte) error {
	if len(b) == 0 {
		return nil
	}

	s := Status{Type: StatusTypeUnknown}
	if er := json.Unmarshal(b, &s); er != nil {
		return er
	}

	if s.Type == StatusTypeError {
		switch s.Code {
		case StatusCodeNotExists:
			return newNotExistsError(s.Message)

		case StatusCodePermissionDenied:
			return newPermissionDeniedError(s.Message)

		default:
			return Feedback(s.Message)
		}

	} else if s.Type == StatusTypeUnknown {
		return errors.New("Unknown status error")
	} else {
		return nil
	}
}

// notExistsError is used when trying to access a file that does not exist.
type notExistsError struct {
	internalError error
}

func newNotExistsError(msg string) notExistsError {
	return notExistsError{
		internalError: errors.New(msg),
	}
}

func (e notExistsError) Error() string {
	return e.internalError.Error()
}

// permissionDeniedError is used when trying to access a file without the permission to it.
type permissionDeniedError struct {
	internalError error
}

func newPermissionDeniedError(msg string) permissionDeniedError {
	return permissionDeniedError{
		internalError: errors.New(msg),
	}
}

func (e permissionDeniedError) Error() string {
	return e.internalError.Error()
}
