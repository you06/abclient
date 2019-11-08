package util

import (
	"fmt"
)

// Version information.
var (
	BuildTS   = "None"
	BuildHash = "None"
)

// PrintInfo prints the octopus version information
func PrintInfo() {
	fmt.Println("Git Commit Hash:", BuildHash)
	fmt.Println("UTC Build Time: ", BuildTS)
}
