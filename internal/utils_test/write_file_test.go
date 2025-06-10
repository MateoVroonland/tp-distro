package utils_test

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/MateoVroonland/tp-distro/internal/utils"
)

func TestWriteFile(t *testing.T) {
	err := utils.AtomicallyWriteFile("test.txt", []byte("test"))
	defer os.Remove("test.txt")
	if err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}

	content, err := os.ReadFile("test.txt")
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}
	if string(content) != "test" {
		t.Fatalf("File content is not correct")
	}
}

func TestWriteFileWithExistingFile(t *testing.T) {
	os.WriteFile("test.txt", []byte("test"), 0644)
	defer os.Remove("test.txt")
	err := utils.AtomicallyWriteFile("test.txt", []byte("test"))
	if err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}
}

// TestAtomicallyWriteFile_NoPartialWrites checks that AtomicallyWriteFile
// does not create partial files during the write operation.
func TestAtomicallyWriteFile_NoPartialWrites(t *testing.T) {
	filename := "test_atomic_no_partial.txt"
	defer os.Remove(filename)

	const fileSize = 1*1024*1024*1024 + 1 // 1GB + 1 byte
	data := make([]byte, fileSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	writerDone := make(chan error, 1)
	go func() {
		writerDone <- utils.AtomicallyWriteFile(filename, data)
	}()

	// Concurrently, we check for the existence of partial files.
	// We loop until the writer goroutine is finished.
	for {
		select {
		case err := <-writerDone:
			if err != nil {
				t.Fatalf("Writer failed: %v", err)
			}
			// Writer is done. Final check to ensure file is correct.
			content, err := os.ReadFile(filename)
			if err != nil {
				t.Fatalf("Failed to read file after write: %v", err)
			}
			if !bytes.Equal(content, data) {
				t.Fatal("Final file content is corrupted.")
			}
			if len(content) != fileSize {
				t.Fatalf("Final file size is incorrect: got %d, want %d", len(content), fileSize)
			}
			return // Test finished successfully.
		default:
			// While the writer is running, check file status.
			fi, err := os.Stat(filename)
			if err != nil {
				if os.IsNotExist(err) {
					time.Sleep(1 * time.Millisecond)
					continue // File doesn't exist yet, which is fine.
				}
				t.Fatalf("Unexpected error stating file: %v", err)
			}

			// Because utils.AtomicallyWriteFile uses an atomic rename, any file
			// that appears at the destination path must be complete.
			if fi.Size() != int64(fileSize) {
				t.Fatalf("Detected a file with partial size: %d. This should not happen with atomic writes.", fi.Size())
			}
			time.Sleep(1 * time.Millisecond)
		}
	}
}

// TestOsWriteFile_ShowsPartialWrites demonstrates that os.WriteFile can
// result in partial writes, making it non-atomic.
func TestOsWriteFile_ShowsPartialWrites(t *testing.T) {
	filename := "test_os_write_partial.txt"
	defer os.Remove(filename)

	const fileSize = 1*1024*1024*1024 + 1 // 1GB + 1 byte
	data := make([]byte, fileSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	writerDone := make(chan error, 1)
	go func() {
		writerDone <- os.WriteFile(filename, data, 0644)
	}()

	// Concurrently, we check for the existence of partial files.
	for {
		select {
		case err := <-writerDone:
			if err != nil {
				t.Fatalf("Writer failed: %v", err)
			}
			// If we reach here, it means we never detected a partial write.
			// This might happen on a very fast system, but it means the test
			// failed to demonstrate the non-atomic nature of os.WriteFile.
			t.Fatalf("Warning: partial write was not detected. The test is not reliably demonstrating the non-atomic nature of os.WriteFile on this system.")
			return
		default:
			// While the writer is running, check file status.
			fi, err := os.Stat(filename)
			if err != nil {
				if os.IsNotExist(err) {
					continue // File doesn't exist yet, which is fine.
				}
				t.Fatalf("Unexpected error stating file: %v", err)
			}

			// With os.WriteFile, it's possible to see the file as it's being written.
			// If we see a file that is not empty but not yet complete, we've found a partial write.
			if fi.Size() > 0 && fi.Size() < int64(fileSize) {
				t.Logf("Detected a partial write of size %d (expected %d). This shows os.WriteFile is not atomic.", fi.Size(), fileSize)
				return
			}
			// time.Sleep(1 * time.Millisecond)
		}
	}
}
