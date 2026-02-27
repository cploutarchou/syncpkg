package sftp

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cploutarchou/syncpkg/worker"
	"github.com/fsnotify/fsnotify"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// SyncDirection is the direction of the sync operation
type SyncDirection int

const (
	//LocalToRemote is the direction of the sync operation from local to remote
	LocalToRemote SyncDirection = iota
	//RemoteToLocal is the direction of the sync operation from remote to local
	RemoteToLocal
)

// Logger is the logger used by the package. It defaults to log.New(os.Stdout, "sftp: ", log.Lshortfile)
var logger = log.New(os.Stdout, "sftp: ", log.Lshortfile)

// SFtp is the struct that holds the sftp client and the sync direction
type SFTP struct {
	//Direction is the direction of the sync operation
	Direction SyncDirection
	//config is the extra configuration for the sftp client
	config *ExtraConfig
	//Watcher is the fsnotify watcher used to watch for file changes
	Watcher *fsnotify.Watcher
	//ctx is the context used to cancel the watcher and the worker pool
	ctx context.Context
	//mu is the mutex used to lock the sftp client when uploading/downloading files
	mu sync.Mutex
	//Client is the sftp client
	Client *sftp.Client
	//Pool is the worker pool
	Pool *worker.Pool
}

// ExtraConfig is the struct that holds the extra configuration for the sftp client
type ExtraConfig struct {
	//Username is the username used to connect to the sftp server
	Username string
	//Password is the password used to connect to the sftp server
	Password string
	//PrivateKeyPassphrase is the passphrase for the private key (used when the key is encrypted)
	PrivateKeyPassphrase string
	//LocalDir is the local directory to sync with the remote directory
	LocalDir string
	//RemoteDir is the remote directory to sync with the local directory
	RemoteDir string
	//Retries is the number of retries to connect to the sftp server
	Retries int
	//MaxRetries is the maximum number of retries to connect to the sftp server
	MaxRetries int
}

// Connect establishes an SFTP connection to the remote server at the specified address and port.
// The function returns an *SFTP object that represents the connection, allowing you to perform file synchronization
// and other SFTP operations between the local and remote directories.
//
// Parameters:
//   - address: The IP address or hostname of the remote SFTP server.
//   - port: The port number to connect to on the remote server.
//   - direction: The direction of the sync operation, either LocalToRemote or RemoteToLocal.
//   - config: An optional *ExtraConfig object that holds additional configuration for the SFTP client.
//     If nil, anonymous authentication will be used. If provided, it may contain the username, password,
//     local directory, remote directory, retries, and max retries for connecting to the SFTP server.
//
// Return Values:
//   - *SFTP: A pointer to the SFTP object representing the connection to the remote server.
//   - error: If an error occurs during the connection process, it will be returned. Otherwise, it will be nil.
//
// Example Usage:
//
//	// Connect to the remote SFTP server using password-based authentication
//	config := &ExtraConfig{
//	  Username:   "your_username",
//	  Password:   "your_password",
//	  LocalDir:   "/path/to/local/directory",
//	  RemoteDir:  "/path/to/remote/directory",
//	  MaxRetries: 3,
//	}
//	sftpConn, err := Connect("example.com", 22, LocalToRemote, config)
//	if err != nil {
//	  log.Fatal("Failed to connect to the SFTP server:", err)
//	}
//	defer sftpConn.Close()
//
//	// Perform SFTP operations, such as initial sync and directory watching
//	sftpConn.WatchDirectory()
func Connect(address string, port int, direction SyncDirection, config *ExtraConfig) (*SFTP, error) {
	var authMethod ssh.AuthMethod
	if config != nil {
		authMethod = ssh.Password(config.Password)
	} else {
		authMethod = ssh.Password("anonymous")
	}

	clientConfig := &ssh.ClientConfig{
		User:            config.Username,
		Auth:            []ssh.AuthMethod{authMethod},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	conn, err := ssh.Dial("tcp", fmt.Sprintf("%s:%d", address, port), clientConfig)
	if err != nil {
		return nil, err
	}

	client, err := sftp.NewClient(conn)
	if err != nil {
		return nil, err
	}

	return &SFTP{
		Client:    client,
		Direction: direction,
		config:    config,
		ctx:       context.Background(),
		Pool:      worker.NewWorkerPool(10),
	}, nil
}

// ConnectSSHPair establishes an SFTP connection to the remote server at the specified address and port
// using SSH key pair authentication. It reads the private key from the current user's home directory
// (typically the `~/.ssh/id_rsa` file) to use for authentication.
//
// The function supports both unencrypted and passphrase-protected private keys. If the private key
// is encrypted, you can provide the passphrase via config.PrivateKeyPassphrase.
//
// The function returns an *SFTP object that represents the connection, allowing you to perform file synchronization
// and other SFTP operations between the local and remote directories.
//
// Parameters:
//   - address: The IP address or hostname of the remote SFTP server.
//   - port: The port number to connect to on the remote server.
//   - direction: The direction of the sync operation, either LocalToRemote or RemoteToLocal.
//   - config: An optional *ExtraConfig object that holds additional configuration for the SFTP client.
//     If nil, default settings will be used. If provided, it may contain the username, local directory,
//     remote directory, retries, max retries, and PrivateKeyPassphrase for encrypted keys.
//
// Return Values:
//   - *SFTP: A pointer to the SFTP object representing the connection to the remote server.
//   - error: If an error occurs during the connection process, it will be returned. Otherwise, it will be nil.
//
// Example Usage:
//
//	// Connect to the remote SFTP server using SSH key pair authentication
//	config := &ExtraConfig{
//	  Username:   "your_username",
//	  LocalDir:   "/path/to/local/directory",
//	  RemoteDir:  "/path/to/remote/directory",
//	  MaxRetries: 3,
//	}
//	sftpConn, err := ConnectSSHPair("example.com", 22, LocalToRemote, config)
//	if err != nil {
//	  log.Fatal("Failed to connect to the SFTP server:", err)
//	}
//	defer sftpConn.Close()
//
// Connect with passphrase-protected private key
//
//	config := &ExtraConfig{
//	  Username:             "your_username",
//	  LocalDir:             "/path/to/local/directory",
//	  RemoteDir:            "/path/to/remote/directory",
//	  PrivateKeyPassphrase: "your_passphrase",
//	}
//	sftpConn, err := ConnectSSHPair("example.com", 22, LocalToRemote, config)
//
//	// Perform SFTP operations, such as initial sync and directory watching
//	sftpConn.WatchDirectory()
func ConnectSSHPair(address string, port int, direction SyncDirection, config *ExtraConfig) (*SFTP, error) {
	usr, err := user.Current()
	if err != nil {
		return nil, fmt.Errorf("cannot get user home directory: %w", err)
	}

	key, err := os.ReadFile(filepath.Join(usr.HomeDir, ".ssh", "id_rsa"))
	if err != nil {
		return nil, fmt.Errorf("unable to read private key: %w", err)
	}

	var signer ssh.Signer
	if config != nil && config.PrivateKeyPassphrase != "" {
		signer, err = ssh.ParsePrivateKeyWithPassphrase(key, []byte(config.PrivateKeyPassphrase))
	} else {
		signer, err = ssh.ParsePrivateKey(key)
	}
	if err != nil {
		return nil, fmt.Errorf("unable to parse private key: %w", err)
	}

	authMethod := ssh.PublicKeys(signer)

	clientConfig := &ssh.ClientConfig{
		User:            config.Username,
		Auth:            []ssh.AuthMethod{authMethod},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	conn, err := ssh.Dial("tcp", fmt.Sprintf("%s:%d", address, port), clientConfig)
	if err != nil {
		return nil, err
	}

	client, err := sftp.NewClient(conn)
	if err != nil {
		return nil, err
	}

	return &SFTP{
		Client:    client,
		Direction: direction,
		config:    config,
		ctx:       context.Background(),
		Pool:      worker.NewWorkerPool(10),
	}, nil
}

// initialSync synchronizes the local directory with the remote directory for the SFTP connection.
// It recursively compares the files and subdirectories in the local and remote directories and performs
// file transfers to ensure that both directories have the same content.
//
// The function returns an error if any issues occur during the synchronization process.
//
// Return Values:
//   - error: If an error occurs during the synchronization process, it will be returned. Otherwise, it will be nil.
func (s *SFTP) initialSync() error {
	return s.syncDir(s.config.LocalDir, s.config.RemoteDir)
}

// syncDir synchronizes the content between the local directory and the remote directory for the SFTP connection.
// The function recursively compares the files and subdirectories in the local and remote directories and performs
// file transfers to ensure that both directories have the same content. The synchronization is based on the
// specified SyncDirection (LocalToRemote or RemoteToLocal) of the SFTP connection.
//
// Parameters:
//   - localDir: The local directory path to synchronize with the remote directory.
//   - remoteDir: The remote directory path to synchronize with the local directory.
//
// Return Values:
//   - error: If an error occurs during the synchronization process, it will be returned. Otherwise, it will be nil.
func (s *SFTP) syncDir(localDir, remoteDir string) error {
	switch s.Direction {
	case LocalToRemote:
		localFiles, err := os.ReadDir(localDir)
		if err != nil {
			return err
		}
		for _, file := range localFiles {
			localFilePath := filepath.Join(localDir, file.Name())
			remoteFilePath := filepath.Join(remoteDir, file.Name())

			if file.IsDir() {
				err = s.checkOrCreateDir(remoteFilePath)
				if err != nil {
					return err
				}
				err = s.syncDir(localFilePath, remoteFilePath)
				if err != nil {
					return err
				}
			} else {
				_, err := s.Client.Stat(remoteFilePath)
				if err != nil {
					err = s.uploadFile(localFilePath)
					if err != nil {
						return err
					}
				}
			}
		}

	case RemoteToLocal:
		remoteFiles, err := s.Client.ReadDir(remoteDir)
		if err != nil {
			return err
		}

		for _, file := range remoteFiles {
			remoteFilePath := filepath.Join(remoteDir, file.Name())
			localFilePath := filepath.Join(localDir, file.Name())

			if file.IsDir() {
				err = s.checkOrCreateDir(localFilePath)
				if err != nil {
					return err
				}
				err = s.syncDir(localFilePath, remoteFilePath)
				if err != nil {
					return err
				}
			} else {
				_, err := os.Stat(localFilePath)
				if err != nil {
					err = s.downloadFile(remoteFilePath)
					if err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

// checkOrCreateDir checks if the specified directory exists. If the directory does not exist, it creates it.
// The behavior of the function depends on the SyncDirection (LocalToRemote or RemoteToLocal) of the SFTP connection.
//
// Parameters:
//   - dirPath: The path of the directory to check or create.
//
// Return Values:
//   - error: If an error occurs while checking or creating the directory, it will be returned. Otherwise, it will be nil.
func (s *SFTP) checkOrCreateDir(dirPath string) error {
	_, err := os.Stat(dirPath)
	if os.IsNotExist(err) {
		if s.Direction == LocalToRemote {
			//create the directory to remote server if it doesn't exist  and all subdirectories
			err := s.Client.MkdirAll(dirPath)
			if err != nil {
				return err
			}
			// set the permissions to 755
			err = s.Client.Chmod(dirPath, 0755)
			if err != nil {
				return err
			}

		} else {
			errDir := os.MkdirAll(dirPath, 0755)
			if errDir != nil {
				return err
			}
		}
	}
	return nil
}

// WatchDirectory sets up a file system watcher to monitor changes in the local or remote directory,
// depending on the SyncDirection of the SFTP connection. When a file or directory event is detected,
// it triggers the corresponding worker to handle the event.
//
// The function first starts the worker pool, performs an initial synchronization of the local and remote
// directories using the initialSync method, and then sets up the file system watcher to watch for changes.
// The watcher is added to the specified local or remote directory, and when a file or directory is created,
// modified, or removed, the corresponding worker is launched to handle the event.
//
// Note: The worker pool must be running before calling this function.
//
// Usage:
//
//	// Assume sftpConn is an established SFTP connection with a worker pool.
//	sftpConn.WatchDirectory()
//
// Example:
//
//	// Create an SFTP connection with a worker pool.
//	config := &ExtraConfig{
//	  Username:    "your_username",
//	  Password:    "your_password",
//	  LocalDir:    "/path/to/local/directory",
//	  RemoteDir:   "/path/to/remote/directory",
//	  Retries:     3,
//	  MaxRetries:  5,
//	}
//	sftpConn, err := Connect("your_server_address", 22, LocalToRemote, config)
//	if err != nil {
//	  log.Fatal("Failed to connect:", err)
//	}
//
// defer sftpConn.Close()
//
//	// Watch for changes in the directory.
//	go sftpConn.WatchDirectory()
func (s *SFTP) WatchDirectory() {
	// Starting the worker pool
	for i := 0; i < cap(s.Pool.Tasks); i++ {
		go s.Worker()
	}
	logger.Println("Starting initial sync...")
	err := s.initialSync()
	if err != nil {
		logger.Fatal(err)
	}
	logger.Println("Initial sync done.")

	logger.Println("Setting up watcher...")
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		logger.Fatal(err)
	}
	defer func(watcher *fsnotify.Watcher) {
		err = watcher.Close()
		if err != nil {
			logger.Println("Error closing watcher:", err)
		}
	}(watcher)

	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				logger.Println("Received event:", event)

				s.Pool.WG.Add(1)
				s.Pool.Tasks <- worker.Task{EventType: event.Op, Name: event.Name}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				logger.Println("Error:", err)
			}
		}
	}()

	logger.Println("Adding directories to watcher...")
	switch s.Direction {
	case LocalToRemote:
		logger.Println("Adding watcher to local directory: ", s.config.LocalDir)
		err = s.AddDirectoriesToWatcher(watcher, s.config.LocalDir)
		if err != nil {
			logger.Fatal(err)
		}
		logger.Println("Starting directory watch...")
	case RemoteToLocal:
		logger.Println("Adding watcher to remote directory: ", s.config.RemoteDir)
		err = s.AddDirectoriesToWatcher(watcher, s.config.RemoteDir)
		if err != nil {
			logger.Fatal(err)
		}
		logger.Println("Starting directory watch...")
	}

	<-s.ctx.Done()
	logger.Println("Directory watch ended.")
}

// AddDirectoriesToWatcher adds the specified directory and its subdirectories to the fsnotify watcher
// based on the SyncDirection of the SFTP connection. For a LocalToRemote connection, it adds the local
// directory and its subdirectories to the watcher. For a RemoteToLocal connection, it dynamically monitors
// the remote directory and its subdirectories by continuously comparing the file modifications between
// successive calls and triggering the corresponding worker to handle the events.
//
// Parameters:
//   - watcher: The fsnotify.Watcher to which the directories should be added.
//   - rootDir: The root directory to start watching.
//
// Note: The function will continuously monitor the directories for changes until the SFTP context is canceled.
func (s *SFTP) AddDirectoriesToWatcher(watcher *fsnotify.Watcher, rootDir string) error {
	switch s.Direction {
	case LocalToRemote:
		return filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
			if info.IsDir() {
				err = watcher.Add(path)
				if err != nil {
					return err
				}
				logger.Println("Adding watcher to directory:", path)
			}
			return nil
		})
	case RemoteToLocal:
		var prevFiles map[string]os.FileInfo
		for {
			// Read the remote directory and its subdirectories.
			newFiles := make(map[string]os.FileInfo)
			err := s.walkRemoteDir(rootDir, newFiles)
			if err != nil {
				return err
			}

			// Check for new or removed files.
			if prevFiles != nil {
				for p, file := range newFiles {
					prevFile, exists := prevFiles[p]
					if !exists || prevFile.ModTime().Before(file.ModTime()) {

						s.Pool.WG.Add(1)

						s.Pool.Tasks <- worker.Task{EventType: fsnotify.Create, Name: p}
						logger.Println("New or modified file:", p)
					}
				}
				for p := range prevFiles {
					_, exists := newFiles[p]
					if !exists {

						s.Pool.WG.Add(1)

						s.Pool.Tasks <- worker.Task{EventType: fsnotify.Remove, Name: p}
						logger.Println("File removed:", p)
					}
				}
			}
			prevFiles = newFiles
			// Wait for a while before checking again.
			time.Sleep(time.Second * 1)
		}
	}
	return nil
}

// uploadFile uploads a file from the local directory to the remote directory using the SFTP client.
// It locks the SFTP client to prevent concurrent uploads and ensures proper cleanup by closing
// the source and destination files after the upload is complete or in case of an error.
//
// Parameters:
//   - filePath: The path of the file in the local directory to upload.
//
// Returns:
//   - error: If an error occurs during the upload process.
//
// Note: This function is meant to be used within the SFTP struct and should not be called directly.
func (s *SFTP) uploadFile(filePath string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	relativePath, err := filepath.Rel(s.config.LocalDir, filePath)
	if err != nil {
		return err
	}

	srcFile, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer func(srcFile *os.File) {
		err = srcFile.Close()
		if err != nil {
			logger.Println("Error closing file:", err)
		}
	}(srcFile)

	dstFile, err := s.Client.Create(filepath.Join(s.config.RemoteDir, relativePath))
	if err != nil {
		return err
	}
	defer func(dstFile *sftp.File) {
		err = dstFile.Close()
		if err != nil {
			logger.Println("Error closing file:", err)
		}
	}(dstFile)

	if s.ctx.Err() != nil {
		return s.ctx.Err()
	}

	_, err = io.Copy(dstFile, srcFile)
	return err
}

// uploadFile uploads a file from the local directory to the remote directory using the SFTP client.
// It locks the SFTP client to prevent concurrent uploads and ensures proper cleanup by closing
// the source and destination files after the upload is complete or in case of an error.
//
// Parameters:
//   - filePath: The path of the file in the local directory to upload.
//
// Returns:
//   - error: If an error occurs during the upload process.
//
// Note: This function is meant to be used within the SFTP struct and should not be called directly.
func (s *SFTP) downloadFile(remotePath string) error {

	if strings.Contains(remotePath, ".swp") {
		return nil
	}
	logger.Println("Downloading file:", remotePath)
	relativePath, err := filepath.Rel(s.config.RemoteDir, remotePath)
	if err != nil {
		return err
	}

	srcFile, err := s.Client.Open(remotePath)
	if err != nil {
		return err
	}
	defer func(srcFile *sftp.File) {
		err = srcFile.Close()
		if err != nil {
			logger.Println("Error closing file:", err)
		}
	}(srcFile)

	dstFile, err := os.Create(filepath.Join(s.config.LocalDir, relativePath))
	if err != nil {
		return err
	}
	defer func(dstFile *os.File) {
		err = dstFile.Close()
		if err != nil {
			logger.Println("Error closing file:", err)
		}
	}(dstFile)

	if s.ctx.Err() != nil {
		return s.ctx.Err()
	}

	_, err = io.Copy(dstFile, srcFile)
	return err
}

// Mkdir creates a directory in the remote server based on the config
// Parameters:
//   - dir: The path of the directory to create.
//
// Returns:
//   - error: If an error occurs during the upload process.
//
// Note: This function is meant to be used within the SFTP struct and should not be called directly.
func (s *SFTP) Mkdir(dir string) error {
	err := s.Client.Mkdir(filepath.Join(s.config.RemoteDir, dir))
	return err
}

// RemoveRemoteFile removes a file from the remote server based on the config and the relative path
// Parameters:
//   - remotePath: The path of the file to remove.
//
// Returns:
//   - error: If an error occurs during the upload process.
//
// Note: This function is meant to be used within the SFTP struct and should not be called directly.
func (s *SFTP) RemoveRemoteFile(remotePath string) error {
	relativePath, err := filepath.Rel(s.config.LocalDir, remotePath)
	if err != nil {
		return err
	}
	toRemotePath := filepath.Join(s.config.RemoteDir, relativePath)
	err = s.Client.Remove(toRemotePath)
	return err
}

// RemoveLocalFile removes a file from the local server based on the config and the relative path
// Parameters:
//   - localPath: The path of the file to remove.
//
// Returns:
//   - error: If an error occurs during the upload process.
//
// Note: This function is meant to be used within the SFTP struct and should not be called directly.
func (s *SFTP) RemoveLocalFile(localPath string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	toLocalPath := s.convertRemoteToLocalPath(localPath)
	err := os.Remove(toLocalPath)
	return err
}

// walkRemoteDir traverses a remote directory and its subdirectories using the SFTP client,
// and adds all files it finds to the provided map.
//
// Parameters:
//   - dir: The path of the remote directory to traverse.
//   - files: A map to store the file paths and their corresponding os.FileInfo.
//
// Returns:
//   - error: If an error occurs during the traversal process.
//
// Note: This function is meant to be used within the SFTP struct and should not be called directly.
func (s *SFTP) walkRemoteDir(dir string, files map[string]os.FileInfo) error {
	entries, err := s.Client.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		join := path.Join(dir, entry.Name())
		if entry.IsDir() {
			err = s.walkRemoteDir(join, files)
			if err != nil {
				return err
			}
		} else {
			files[join] = entry

		}
	}

	return nil
}

// convertRemoteToLocalPath converts the remote path to a local path based on the config
// Parameters:
//   - remotePath: The path of the file to convert.
//
// Returns:
//   - localPath: The converted path
//
// Note: This function is meant to be used within the SFTP struct and should not be called directly.
func (s *SFTP) convertRemoteToLocalPath(remotePath string) string {
	relativePath, _ := filepath.Rel(s.config.RemoteDir, remotePath)
	localPath := filepath.Join(s.config.LocalDir, relativePath)
	return localPath
}

// Worker starts a new worker goroutine that processes tasks received from the worker pool's task channel.
// The tasks can include file events such as creation, write, and removal events received from the
// fsnotify watcher.
//
// Note: This function is meant to be used within the SFTP struct and should not be called directly.
func (s *SFTP) Worker() {
	for task := range s.Pool.Tasks {
		switch task.EventType {
		case fsnotify.Create:
			switch s.Direction {
			case LocalToRemote:
				err := s.uploadFile(task.Name)
				if err != nil {
					logger.Println("Error uploading file:", err)
				}
			case RemoteToLocal:
				err := s.downloadFile(task.Name)
				if err != nil {
					logger.Println("Error downloading file:", err)
				}
			}
		case fsnotify.Write:
			err := s.uploadFile(task.Name)
			if err != nil {
				logger.Println("Error uploading file:", err)
			}
		case fsnotify.Remove:
			switch s.Direction {
			case LocalToRemote:
				err := s.RemoveRemoteFile(task.Name)
				if err != nil {
					logger.Println("Error deleting file:", err)
				}
			case RemoteToLocal:
				err := s.RemoveLocalFile(task.Name)
				if err != nil {
					logger.Println("Error removing remote file:", err)
				}
			}
		}
		s.Pool.WG.Done()
	}
}
