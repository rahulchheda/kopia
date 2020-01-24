package walker

import (
	"fmt"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	Exe := os.Getenv("WALKER_EXE")
	if Exe == "" {
		fmt.Println("Skipping fio tests if WALKER_EXE is not set")
		os.Exit(0)
	}

	result := m.Run()

	os.Exit(result)
}
