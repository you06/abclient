package util

import (
	"fmt"
	"github.com/juju/errors"
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

// ErrorMustSame return error if both error not same
func ErrorMustSame(err1, err2 error) error {
	if (err1 == nil) != (err2 == nil) {
		return errors.Errorf("error not same, got err1: %v and err2:L %v", err1, err2)
	}
	return nil
}
