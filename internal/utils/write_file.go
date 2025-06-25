package utils

import "os"

func AtomicallyWriteFile(path string, data []byte) error {
	tmpFile, err := os.CreateTemp("/data", "tmp_file_*")
	if err != nil {
		return err
	}
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.Write(data)
	if err != nil {
		return err
	}

	err = tmpFile.Sync()
	if err != nil {
		return err
	}

	err = tmpFile.Close()
	if err != nil {
		return err
	}

	return os.Rename(tmpFile.Name(), path)
}
