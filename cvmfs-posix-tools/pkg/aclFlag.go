package pkg

import (
	"fmt"
	"strings"
)

type ACLFlag string

const (
	ACLPreserveAll   ACLFlag = "preserve-all"
	ACLPreserveMode  ACLFlag = "preserve-mode"
	ACLPreserveExec  ACLFlag = "preserve-execute"
	ACLPreserveOwner ACLFlag = "preserve-owner"
	ACLNone          ACLFlag = "none"
)

func (a *ACLFlag) Type() string {
	return "ACLFlag"
}

func (a *ACLFlag) String() string {
	return string(*a)
}

func (a *ACLFlag) Set(value string) error {
	lowered := strings.ToLower(value)
	switch ACLFlag(lowered) {
	case ACLPreserveAll, ACLPreserveMode, ACLPreserveExec, ACLPreserveOwner, ACLNone:
		*a = ACLFlag(lowered)
		return nil
	default:
		return fmt.Errorf("invalid value \"%s\" for -a/--acls flag", value)
	}
}
