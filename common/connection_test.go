package common

import (
	"os"
	"path"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/pkg/sftp"
	"github.com/stretchr/testify/assert"

	"github.com/drakkan/sftpgo/dataprovider"
	"github.com/drakkan/sftpgo/vfs"
)

// MockOsFs mockable OsFs
type MockOsFs struct {
	vfs.Fs
	hasVirtualFolders bool
}

// Name returns the name for the Fs implementation
func (fs MockOsFs) Name() string {
	return "mockOsFs"
}

// HasVirtualFolders returns true if folders are emulated
func (fs MockOsFs) HasVirtualFolders() bool {
	return fs.hasVirtualFolders
}

func (fs MockOsFs) IsUploadResumeSupported() bool {
	return !fs.hasVirtualFolders
}

func newMockOsFs(hasVirtualFolders bool, connectionID, rootDir string) vfs.Fs {
	return &MockOsFs{
		Fs:                vfs.NewOsFs(connectionID, rootDir, ""),
		hasVirtualFolders: hasVirtualFolders,
	}
}

func TestRemoveErrors(t *testing.T) {
	mappedPath := filepath.Join(os.TempDir(), "map")
	homePath := filepath.Join(os.TempDir(), "home")

	user := dataprovider.User{
		Username: "remove_errors_user",
		HomeDir:  homePath,
		VirtualFolders: []vfs.VirtualFolder{
			{
				BaseVirtualFolder: vfs.BaseVirtualFolder{
					Name:       filepath.Base(mappedPath),
					MappedPath: mappedPath,
				},
				VirtualPath: "/virtualpath",
			},
		},
	}
	user.Permissions = make(map[string][]string)
	user.Permissions["/"] = []string{dataprovider.PermAny}
	fs := vfs.NewOsFs("", os.TempDir(), "")
	conn := NewBaseConnection("", ProtocolFTP, "", user)
	err := conn.IsRemoveDirAllowed(fs, mappedPath, "/virtualpath1")
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "permission denied")
	}
	err = conn.RemoveFile(fs, filepath.Join(homePath, "missing_file"), "/missing_file",
		vfs.NewFileInfo("info", false, 100, time.Now(), false))
	assert.Error(t, err)
}

func TestSetStatMode(t *testing.T) {
	oldSetStatMode := Config.SetstatMode
	Config.SetstatMode = 1

	fakePath := "fake path"
	user := dataprovider.User{
		HomeDir: os.TempDir(),
	}
	user.Permissions = make(map[string][]string)
	user.Permissions["/"] = []string{dataprovider.PermAny}
	fs := newMockOsFs(true, "", user.GetHomeDir())
	conn := NewBaseConnection("", ProtocolWebDAV, "", user)
	err := conn.handleChmod(fs, fakePath, fakePath, nil)
	assert.NoError(t, err)
	err = conn.handleChown(fs, fakePath, fakePath, nil)
	assert.NoError(t, err)
	err = conn.handleChtimes(fs, fakePath, fakePath, nil)
	assert.NoError(t, err)

	Config.SetstatMode = 2
	err = conn.handleChmod(fs, fakePath, fakePath, nil)
	assert.NoError(t, err)

	Config.SetstatMode = oldSetStatMode
}

func TestRecursiveRenameWalkError(t *testing.T) {
	fs := vfs.NewOsFs("", os.TempDir(), "")
	conn := NewBaseConnection("", ProtocolWebDAV, "", dataprovider.User{})
	err := conn.checkRecursiveRenameDirPermissions(fs, fs, "/source", "/target")
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestCrossRenameFsErrors(t *testing.T) {
	fs := vfs.NewOsFs("", os.TempDir(), "")
	conn := NewBaseConnection("", ProtocolWebDAV, "", dataprovider.User{})
	res := conn.hasSpaceForCrossRename(fs, vfs.QuotaCheckResult{}, 1, "missingsource")
	assert.False(t, res)
	if runtime.GOOS != osWindows {
		dirPath := filepath.Join(os.TempDir(), "d")
		err := os.Mkdir(dirPath, os.ModePerm)
		assert.NoError(t, err)
		err = os.Chmod(dirPath, 0001)
		assert.NoError(t, err)

		res = conn.hasSpaceForCrossRename(fs, vfs.QuotaCheckResult{}, 1, dirPath)
		assert.False(t, res)

		err = os.Chmod(dirPath, os.ModePerm)
		assert.NoError(t, err)
		err = os.Remove(dirPath)
		assert.NoError(t, err)
	}
}

func TestRenameVirtualFolders(t *testing.T) {
	vdir := "/avdir"
	u := dataprovider.User{}
	u.VirtualFolders = append(u.VirtualFolders, vfs.VirtualFolder{
		BaseVirtualFolder: vfs.BaseVirtualFolder{
			Name:       "name",
			MappedPath: "mappedPath",
		},
		VirtualPath: vdir,
	})
	fs := vfs.NewOsFs("", os.TempDir(), "")
	conn := NewBaseConnection("", ProtocolFTP, "", u)
	res := conn.isRenamePermitted(fs, fs, "source", "target", vdir, "vdirtarget", nil)
	assert.False(t, res)
}

func TestUpdateQuotaAfterRename(t *testing.T) {
	user := dataprovider.User{
		Username: userTestUsername,
		HomeDir:  filepath.Join(os.TempDir(), "home"),
	}
	mappedPath := filepath.Join(os.TempDir(), "vdir")
	user.Permissions = make(map[string][]string)
	user.Permissions["/"] = []string{dataprovider.PermAny}
	user.VirtualFolders = append(user.VirtualFolders, vfs.VirtualFolder{
		BaseVirtualFolder: vfs.BaseVirtualFolder{
			MappedPath: mappedPath,
		},
		VirtualPath: "/vdir",
		QuotaFiles:  -1,
		QuotaSize:   -1,
	})
	user.VirtualFolders = append(user.VirtualFolders, vfs.VirtualFolder{
		BaseVirtualFolder: vfs.BaseVirtualFolder{
			MappedPath: mappedPath,
		},
		VirtualPath: "/vdir1",
		QuotaFiles:  -1,
		QuotaSize:   -1,
	})
	err := os.MkdirAll(user.GetHomeDir(), os.ModePerm)
	assert.NoError(t, err)
	err = os.MkdirAll(mappedPath, os.ModePerm)
	assert.NoError(t, err)
	fs, err := user.GetFilesystem("id")
	assert.NoError(t, err)
	c := NewBaseConnection("", ProtocolSFTP, "", user)
	request := sftp.NewRequest("Rename", "/testfile")
	if runtime.GOOS != osWindows {
		request.Filepath = "/dir"
		request.Target = path.Join("/vdir", "dir")
		testDirPath := filepath.Join(mappedPath, "dir")
		err := os.MkdirAll(testDirPath, os.ModePerm)
		assert.NoError(t, err)
		err = os.Chmod(testDirPath, 0001)
		assert.NoError(t, err)
		err = c.updateQuotaAfterRename(fs, request.Filepath, request.Target, testDirPath, 0)
		assert.Error(t, err)
		err = os.Chmod(testDirPath, os.ModePerm)
		assert.NoError(t, err)
	}
	testFile1 := "/testfile1"
	request.Target = testFile1
	request.Filepath = path.Join("/vdir", "file")
	err = c.updateQuotaAfterRename(fs, request.Filepath, request.Target, filepath.Join(mappedPath, "file"), 0)
	assert.Error(t, err)
	err = os.WriteFile(filepath.Join(mappedPath, "file"), []byte("test content"), os.ModePerm)
	assert.NoError(t, err)
	request.Filepath = testFile1
	request.Target = path.Join("/vdir", "file")
	err = c.updateQuotaAfterRename(fs, request.Filepath, request.Target, filepath.Join(mappedPath, "file"), 12)
	assert.NoError(t, err)
	err = os.WriteFile(filepath.Join(user.GetHomeDir(), "testfile1"), []byte("test content"), os.ModePerm)
	assert.NoError(t, err)
	request.Target = testFile1
	request.Filepath = path.Join("/vdir", "file")
	err = c.updateQuotaAfterRename(fs, request.Filepath, request.Target, filepath.Join(mappedPath, "file"), 12)
	assert.NoError(t, err)
	request.Target = path.Join("/vdir1", "file")
	request.Filepath = path.Join("/vdir", "file")
	err = c.updateQuotaAfterRename(fs, request.Filepath, request.Target, filepath.Join(mappedPath, "file"), 12)
	assert.NoError(t, err)

	err = os.RemoveAll(mappedPath)
	assert.NoError(t, err)
	err = os.RemoveAll(user.GetHomeDir())
	assert.NoError(t, err)
}

func TestErrorsMapping(t *testing.T) {
	fs := vfs.NewOsFs("", os.TempDir(), "")
	conn := NewBaseConnection("", ProtocolSFTP, "", dataprovider.User{HomeDir: os.TempDir()})
	for _, protocol := range supportedProtocols {
		conn.SetProtocol(protocol)
		err := conn.GetFsError(fs, os.ErrNotExist)
		if protocol == ProtocolSFTP {
			assert.EqualError(t, err, sftp.ErrSSHFxNoSuchFile.Error())
		} else if protocol == ProtocolWebDAV || protocol == ProtocolFTP || protocol == ProtocolHTTP {
			assert.EqualError(t, err, os.ErrNotExist.Error())
		} else {
			assert.EqualError(t, err, ErrNotExist.Error())
		}
		err = conn.GetFsError(fs, os.ErrPermission)
		if protocol == ProtocolSFTP {
			assert.EqualError(t, err, sftp.ErrSSHFxPermissionDenied.Error())
		} else {
			assert.EqualError(t, err, ErrPermissionDenied.Error())
		}
		err = conn.GetFsError(fs, os.ErrClosed)
		if protocol == ProtocolSFTP {
			assert.EqualError(t, err, sftp.ErrSSHFxFailure.Error())
		} else {
			assert.EqualError(t, err, ErrGenericFailure.Error())
		}
		err = conn.GetFsError(fs, ErrPermissionDenied)
		if protocol == ProtocolSFTP {
			assert.EqualError(t, err, sftp.ErrSSHFxFailure.Error())
		} else {
			assert.EqualError(t, err, ErrPermissionDenied.Error())
		}
		err = conn.GetFsError(fs, vfs.ErrVfsUnsupported)
		if protocol == ProtocolSFTP {
			assert.EqualError(t, err, sftp.ErrSSHFxOpUnsupported.Error())
		} else {
			assert.EqualError(t, err, ErrOpUnsupported.Error())
		}
		err = conn.GetFsError(fs, vfs.ErrStorageSizeUnavailable)
		if protocol == ProtocolSFTP {
			assert.EqualError(t, err, sftp.ErrSSHFxOpUnsupported.Error())
		} else {
			assert.EqualError(t, err, vfs.ErrStorageSizeUnavailable.Error())
		}
		err = conn.GetFsError(fs, nil)
		assert.NoError(t, err)
		err = conn.GetOpUnsupportedError()
		if protocol == ProtocolSFTP {
			assert.EqualError(t, err, sftp.ErrSSHFxOpUnsupported.Error())
		} else {
			assert.EqualError(t, err, ErrOpUnsupported.Error())
		}
	}
}

func TestMaxWriteSize(t *testing.T) {
	permissions := make(map[string][]string)
	permissions["/"] = []string{dataprovider.PermAny}
	user := dataprovider.User{
		Username:    userTestUsername,
		Permissions: permissions,
		HomeDir:     filepath.Clean(os.TempDir()),
	}
	fs, err := user.GetFilesystem("123")
	assert.NoError(t, err)
	conn := NewBaseConnection("", ProtocolFTP, "", user)
	quotaResult := vfs.QuotaCheckResult{
		HasSpace: true,
	}
	size, err := conn.GetMaxWriteSize(quotaResult, false, 0, fs.IsUploadResumeSupported())
	assert.NoError(t, err)
	assert.Equal(t, int64(0), size)

	conn.User.Filters.MaxUploadFileSize = 100
	size, err = conn.GetMaxWriteSize(quotaResult, false, 0, fs.IsUploadResumeSupported())
	assert.NoError(t, err)
	assert.Equal(t, int64(100), size)

	quotaResult.QuotaSize = 1000
	size, err = conn.GetMaxWriteSize(quotaResult, false, 50, fs.IsUploadResumeSupported())
	assert.NoError(t, err)
	assert.Equal(t, int64(100), size)

	quotaResult.QuotaSize = 1000
	quotaResult.UsedSize = 990
	size, err = conn.GetMaxWriteSize(quotaResult, false, 50, fs.IsUploadResumeSupported())
	assert.NoError(t, err)
	assert.Equal(t, int64(60), size)

	quotaResult.QuotaSize = 0
	quotaResult.UsedSize = 0
	size, err = conn.GetMaxWriteSize(quotaResult, true, 100, fs.IsUploadResumeSupported())
	assert.EqualError(t, err, ErrQuotaExceeded.Error())
	assert.Equal(t, int64(0), size)

	size, err = conn.GetMaxWriteSize(quotaResult, true, 10, fs.IsUploadResumeSupported())
	assert.NoError(t, err)
	assert.Equal(t, int64(90), size)

	fs = newMockOsFs(true, fs.ConnectionID(), user.GetHomeDir())
	size, err = conn.GetMaxWriteSize(quotaResult, true, 100, fs.IsUploadResumeSupported())
	assert.EqualError(t, err, ErrOpUnsupported.Error())
	assert.Equal(t, int64(0), size)
}
