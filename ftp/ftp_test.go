package ftp

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ory/dockertest"
	"github.com/ory/dockertest/docker"
)

func setupFtpServer(t *testing.T) (string, int, *dockertest.Resource) {
	log.Println("Setting up FTP server...")
	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Fatalf("Could not connect to docker: %s", err)
	}

	options := &dockertest.RunOptions{
		Repository: "stilliard/pure-ftpd",
		Tag:        "latest",
		Env:        []string{"PUBLICHOST=0.0.0.0", "FTP_USER_NAME=foo", "FTP_USER_PASS=pass", "FTP_USER_HOME=/home/foo"},
	}
	options.ExposedPorts = []string{"21/tcp"}

	options.PortBindings = map[docker.Port][]docker.PortBinding{
		"21/tcp": {{HostIP: "0.0.0.0", HostPort: "21/tcp"}},
	}

	resource, err := pool.RunWithOptions(options)
	if err != nil {
		t.Fatalf("Could not start resource: %s", err)
	}
	time.Sleep(10 * time.Second)
	return "0.0.0.0", 21, resource

}

func teardownFtpServer(t *testing.T, resource *dockertest.Resource) {
	log.Println("Tearing down FTP server...")
	if err := resource.Close(); err != nil {
		t.Fatalf("Could not stop resource: %s", err)
	}
}

func TestLogin(t *testing.T) {
	log.Println("Running TestLogin...")
	address, port, resource := setupFtpServer(t)
	defer teardownFtpServer(t, resource)
	time.Sleep(10 * time.Second)

	config := &ExtraConfig{
		Username:   "foo",
		Password:   "pass",
		LocalDir:   "./tmp",
		RemoteDir:  "/home/foo/upload",
		Retries:    3,
		MaxRetries: 3,
	}
	ftp, err := Connect(address, port, LocalToRemote, config)

	if err != nil {
		t.Fatalf("Connect returned an error: %v", err)
	}

	if ftp == nil {
		t.Fatalf("Connect returned nil FTP")
	}

	log.Println("TestLogin completed successfully.")
}

func TestWatchDirectory(t *testing.T) {
	log.Println("Running TestWatchDirectory...")
	address, port, resource := setupFtpServer(t)
	defer teardownFtpServer(t, resource)
	time.Sleep(10 * time.Second)
	conf := &ExtraConfig{
		Username:   "foo",
		Password:   "pass",
		Retries:    3,
		MaxRetries: 3,
		RemoteDir:  "/home/foo/upload",
		LocalDir:   "./tmp",
	}

	log.Printf("Connecting to FTP server at address %s...\n", address)
	ftpClient, err := Connect(address, port, LocalToRemote, conf)
	if err != nil {
		t.Fatalf("Connect returned an error: %v", err)
	}

	if ftpClient == nil {
		t.Fatalf("Connect returned nil FTP")
	}

	dirToWatch := "./tmp"
	err = os.MkdirAll(dirToWatch, os.ModePerm)
	if err != nil {
		t.Fatalf("Failed to create directory to watch: %v", err)
	}
	t.Logf("Created directory to watch: %s\n", dirToWatch)

	go ftpClient.WatchDirectory()

	time.Sleep(20 * time.Second)

	fileName := "test.txt"
	filePath := filepath.Join(dirToWatch, fileName)

	t.Logf("Creating file: %s\n", filePath)
	file, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	_, err = file.WriteString("test")
	if err != nil {
		t.Fatalf("Failed to write to file: %v", err)
	}
	err = file.Close()
	if err != nil {
		t.Fatalf("Failed to close file: %v", err)
	}
	t.Log("File created and closed.")

	time.Sleep(1 * time.Second)

	fmt.Println("Checking remote file status...")
	remoteFilePath := filepath.Join(conf.RemoteDir, fileName)
	log.Printf("Checking remote file status for: %s\n", remoteFilePath)
	_, err = ftpClient.Stat(remoteFilePath)
	if err != nil {
		t.Fatalf("Failed to stat file: %v", err)
	}

	log.Printf("Removing directory: %s\n", dirToWatch)
	err = os.RemoveAll(dirToWatch)
	if err != nil {
		t.Fatalf("Failed to remove directory: %v", err)
	}
	log.Println("Directory removed.")

	time.Sleep(1 * time.Second)

	log.Println("TestWatchDirectory completed successfully.")
}
