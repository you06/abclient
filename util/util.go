package util

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/go-sql-driver/mysql"
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
	if err1 == nil && err2 == nil {
		return nil
	}

	if (err1 == nil) != (err2 == nil) {
		return errors.Errorf("error not same, got err1: %v and err2: %v", err1, err2)
	}

	myerr1, ok1 := err1.(*mysql.MySQLError)
	myerr2, ok2 := err2.(*mysql.MySQLError)
	log.Info("ok status", ok1, ok2)
	if ok1 != ok2 {
		return errors.Errorf("error type not same, if mysql error err1: %t, err2: %t", ok1, ok2)
	}
	// both other type error
	if !ok1 && !ok2 {
		return nil
	}

	if myerr1.Number != myerr2.Number {
			return errors.Errorf("error number not same, got err1: %v and err2 %v", err1, err2)
	}

	return nil
}
