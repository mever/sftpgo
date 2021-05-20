// +build clifs

package vfs

import (
	"context"
	"encoding/json"
	"github.com/drakkan/sftpgo/logger"
	"github.com/drakkan/sftpgo/metrics"
	"github.com/eikenb/pipeat"
	"github.com/pkg/errors"
	"github.com/pkg/sftp"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	// cliFsName is the name for the Command Line Interface Fs implementation
	cliFsName = "clifs"
)

type CliFsConfig struct {

	// BinPath is the path to a binary that will receive filesystem operations.
	BinPath string
}

func (c *CliFsConfig) isEqual(other *CliFsConfig) bool {
	return c.BinPath == other.BinPath
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
	mountPath      string
	config *CliFsConfig
}

func NewCLIFs(connectionID string, mountPath, localTempDir string, config CliFsConfig) (*CliFs, error) {
	return &CliFs{
		name: cliFsName,
		connectionID: connectionID,
		mountPath: mountPath,
		localTempDir: localTempDir,
		config: &config,
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
	m, er := fs.callMap("stat", name)
	if er != nil {
		return nil, errors.Wrap(er, "calling command failed")
	}

	return newFileInfoFromMap(m)
}

// Lstat returns a FileInfo describing the named file
func (fs *CliFs) Lstat(name string) (os.FileInfo, error) {
	m, er := fs.callMap("lstat", name)
	if er != nil {
		return nil, errors.Wrap(er, "calling command failed")
	}

	return newFileInfoFromMap(m)
}

// Open opens the named file for reading
func (fs *CliFs) Open(name string, offset int64) (File, *pipeat.PipeReaderAt, func(), error) {
	ctx, cancelFn := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, name, strconv.Itoa(int(offset)))
	stdout, er := cmd.StdoutPipe()
	if er != nil {
		cancelFn()
		return nil, nil, nil, errors.Wrap(er, "could not open stdout pipe to command process")
	}
	if er := cmd.Start(); er != nil {
		cancelFn()
		_ = stdout.Close()
		return nil, nil, nil, errors.Wrap(er, "failed to start command process")
	}

	// Assume command's output is prefixed with JSON data about the file, the rest of the data is file content.
	fileMetaData := make(map[string]interface{})
	d := json.NewDecoder(stdout)
	if err := d.Decode(&fileMetaData); err != nil {
		cancelFn()
		_ = stdout.Close()
		return nil, nil, nil, errors.Wrap(err, "could not decode file metadata as JSON")
	}

	r, w, er := pipeat.PipeInDir(fs.localTempDir)
	if er != nil {
		cancelFn()
		_ = stdout.Close()
		return nil, nil, nil, errors.Wrap(er, "failed to create pipeat")
	}

	go func() {
		defer func() {
			cancelFn()
			_ = stdout.Close()
			_ = w.Close()
		}()
		n, err := io.Copy(w, io.MultiReader(d.Buffered(), stdout))
		_ = w.CloseWithError(errors.Wrap(err, "failed to copy"))
		fsLog(fs, logger.LevelDebug, "download completed, path: %#v size: %v, err: %v", name, n, err)
		metrics.GCSTransferCompleted(n, 1, err)
	}()

	return nil, r, cancelFn, errors.Wrap(cmd.Wait(), "wait failed")
}

// Create creates or opens the named file for writing
func (fs *CliFs) Create(name string, flag int) (File, *PipeWriter, func(), error) {
	r, w, err := pipeat.PipeInDir(fs.localTempDir)
	if err != nil {
		return nil, nil, nil, err
	}
	p := NewPipeWriter(w)
	ctx, cancelFn := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, name, strconv.Itoa(flag))
	cmd.Stdin = r
	if er := cmd.Start(); er != nil {
		cancelFn()
		_ = p.Close()
		_ = r.Close()
		return nil, nil, nil, errors.Wrap(er, "failed to start command")
	}
	go func() {
		defer cancelFn()
		err = errors.Wrap(cmd.Wait(), "wait failure")
		n := r.GetReadedBytes()
		r.CloseWithError(err) //nolint:errcheck
		p.Done(err)
		fsLog(fs, logger.LevelDebug, "upload completed, path: %#v, readed bytes: %v, err: %v", name, n, err)
		metrics.GCSTransferCompleted(n, 0, err)
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
	m, er := fs.callMap("remove", source, target)
	if er != nil {
		return errors.Wrap(er, "calling command failed")
	}
	return newStatusFromMap(m)
}

// Remove removes the named file or (empty) directory.
func (fs *CliFs) Remove(name string, isDir bool) error {
	isDirStr := "0"
	if isDir {
		isDirStr = "1"
	}
	m, er := fs.callMap("remove", name, isDirStr)
	if er != nil {
		return errors.Wrap(er, "calling command failed")
	}
	return newStatusFromMap(m)
}

// Mkdir creates a new directory with the specified name and default permissions
func (*CliFs) Mkdir(name string) error {
	return ErrVfsUnsupported
}

// ReadDir reads the directory named by dirname and returns
// a list of directory entries.
func (fs *CliFs) ReadDir(dirname string) ([]os.FileInfo, error) {
	m, er := fs.callMap("readDir", dirname)
	if er != nil {
		return nil, errors.Wrap(er, "calling command failed")
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
	return false
}

// IsPermission returns a boolean indicating whether the error is known to
// report that permission is denied.
func (*CliFs) IsPermission(err error) bool {
	return false
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
	m, er := fs.callMap("getMimeType", name)
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

func (fs CliFs) callMap(name string, args ...string) (map[string]interface{}, error) {
	cmd := exec.Command(name, args...)
	b, er := cmd.Output()
	if er != nil {
		return nil, errors.Wrap(er, "failed to run command: " + fs.config.BinPath + " " + strings.Join(args, " "))
	}

	m := make(map[string]interface{})
	if er = json.Unmarshal(b, &m); er != nil {
		return nil, errors.Wrap(er, "failed to decode JSON")
	}

	return m, nil
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
	fi = NewFileInfo(m["name"].(string), m["isDirectory"].(bool),
		m["sizeInBytes"].(int64), m["modTime"].(time.Time), m["fullName"].(bool))

	return
}

const (
	StatusTypeSuccess int = iota
	StatusTypeWarning
	StatusTypeError
	StatusTypeUnknown
)

type Status struct {
	Type int
	Message string
}

func (s Status) Error() string {
	return s.Message
}

func newStatusFromMap(m map[string]interface{}) *Status {
	if data, has := m["status"]; has {
		if statusData, ok := data.(map[string]interface{}); ok {
			status := &Status{Type: StatusTypeUnknown}
			if t, ok := statusData["type"].(int); ok {
				status.Type = t
			}
			if msg, ok := statusData["message"].(string); ok {
				status.Message = msg
			}
			if status.Type > 1 {
				return status
			} else {
				return nil
			}
		} else {
			return &Status{Type: StatusTypeError, Message: "expected 'status' field to contain status data"}
		}
	}
	return nil
}
