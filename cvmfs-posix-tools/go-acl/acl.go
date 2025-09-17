// Copyright (c) 2015 Joseph Naegele. See LICENSE file.

// Package acl provides an interface to Posix.1e Access Control Lists
// as well as additional ACL implementations (NFS).
package acl

// #ifdef __APPLE__
//  #include <sys/types.h>
// #endif
// #include <sys/acl.h>
// #include <acl/libacl.h>
// #cgo linux LDFLAGS: -lacl
//
// /*
//  * FreeBSD does not contain acl_size and even when it was there, it seemd
//  * to have been non-functional anyway. See FreeBSD r274722.
//  */
// #ifdef __FreeBSD__
// #include <errno.h>
// # endif
// ssize_t acl_size_wrapper(acl_t acl) {
// #ifdef __FreeBSD__
//     errno = ENOSYS;
//     return (-1);
// #else
//     return acl_size(acl);
// #endif
// }
import "C"

import (
	"fmt"
	"syscall"
	"unsafe"
	"os"
)

const (
	otherExec  = 1 << iota
	otherWrite = 1 << iota
	otherRead  = 1 << iota
	groupExec  = 1 << iota
	groupWrite = 1 << iota
	groupRead  = 1 << iota
	userExec   = 1 << iota
	userWrite  = 1 << iota
	userRead   = 1 << iota

	TextSomeEffective = C.TEXT_SOME_EFFECTIVE
	TextNumericIDs    = C.TEXT_NUMERIC_IDS
)

// UID/GID values are returned as ints in package "os".
type Uid int
type Gid int

type Tag int
type Type int

// ACL represents an Access Control List.
type ACL struct {
	a C.acl_t
}

// DeleteDefaultACL removes the default ACL from the specified path.
// Unsupported on Mac OS X.
func DeleteDefaultACL(path string) error {
	rv, err := C.acl_delete_def_file(C.CString(path))
	if rv < 0 {
		errno, _ := err.(syscall.Errno)
		return fmt.Errorf("unable to delete default ACL from file: %w", errno)
	}
	return nil
}

// Unsupported on Mac OS X?
func (acl *ACL) CalcMask() error {
	rv, err := C.acl_calc_mask(&acl.a)
	if rv < 0 {
		errno, _ := err.(syscall.Errno)
		return fmt.Errorf("unable to calculate mask: %w", errno)
	}
	return nil
}

// String returns the string representation of the ACL.
func (acl *ACL) String() string {
	return acl.StringWithOptions(TextSomeEffective) // consistent with libacl's acl_to_text()
}

// StringWithOptions returns the string representation of the ACL based on the options provided.
func (acl *ACL) StringWithOptions(options int) string {
	cs, _ := C.acl_to_any_text(acl.a, nil, '\n', C.int(options))
	if cs == nil {
		return ""
	}
	defer C.acl_free(unsafe.Pointer(cs))
	return C.GoString(cs)
}

// Valid checks if the ACL is valid.
func (acl *ACL) Valid() bool {
	rv := C.acl_valid(acl.a)
	if rv < 0 {
		return false
	}
	return true
}

// CreateEntry creates a new, empty Entry in the ACL.
func (acl *ACL) CreateEntry() (*Entry, error) {
	var e C.acl_entry_t
	rv, err := C.acl_create_entry(&acl.a, &e)
	if rv < 0 {
		errno, _ := err.(syscall.Errno)
		return nil, fmt.Errorf("unable to create entry: %w", errno)
	}
	return &Entry{e}, nil
}

// AddEntry adds an Entry to the ACL.
func (acl *ACL) AddEntry(entry *Entry) error {
	newEntry, err := acl.CreateEntry()
	if err != nil {
		return err
	}
	rv, err := C.acl_copy_entry(newEntry.e, entry.e)
	if rv < 0 {
		errno, _ := err.(syscall.Errno)
		return fmt.Errorf("unable to copy entry while adding new entry: %w", errno)
	}
	return nil
}

// DeleteEntry removes a specific Entry from the ACL.
func (acl *ACL) DeleteEntry(entry *Entry) error {
	rv, err := C.acl_delete_entry(acl.a, entry.e)
	if rv < 0 {
		errno, _ := err.(syscall.Errno)
		return fmt.Errorf("unable to delete entry: %w", errno)
	}
	return nil
}

// Dup makes a copy of the ACL.
func (acl *ACL) Dup() (*ACL, error) {
	cdup, err := C.acl_dup(acl.a)
	if cdup == nil {
		errno, _ := err.(syscall.Errno)
		return nil, fmt.Errorf("unable to dup ACL: %w", errno)
	}
	return &ACL{cdup}, nil
}

// New returns a new, initialized ACL.
func New() (*ACL, error) {
	cacl, err := C.acl_init(C.int(1))
	if cacl == nil {
		// If acl_init fails, *ACL is invalid
		errno, _ := err.(syscall.Errno)
		return nil, fmt.Errorf("unable to create ACL: %w", errno)
	}
	return &ACL{cacl}, nil
}

// FirstEntry returns the first entry in the ACL,
// or nil of there are no more entries.
func (acl *ACL) FirstEntry() *Entry {
	var e C.acl_entry_t
	rv, _ := C.acl_get_entry(acl.a, C.ACL_FIRST_ENTRY, &e)
	if rv <= 0 {
		// either error obtaining entry or entries at all
		return nil
	}
	return &Entry{e}
}

// NextEntry returns the next entry in the ACL,
// or nil of there are no more entries.
func (acl *ACL) NextEntry() *Entry {
	var e C.acl_entry_t
	rv, _ := C.acl_get_entry(acl.a, C.ACL_NEXT_ENTRY, &e)
	if rv <= 0 {
		// either error obtaining entry or no more entries
		return nil
	}
	return &Entry{e}
}

func (acl *ACL) setFile(path string, tp C.acl_type_t) error {
	if !acl.Valid() {
		if err := acl.addBaseEntries(path); err != nil {
			return err
		}
		if !acl.Valid() {
			return fmt.Errorf("Invalid ACL: %s", acl)
		}
	}

	rv, err := C.acl_set_file(C.CString(path), tp, acl.a)
	if rv < 0 {
		errno, _ := err.(syscall.Errno)
		return fmt.Errorf("unable to apply ACL to file: %w", errno)
	}
	return nil
}

// SetFileAccess applies the access ACL to a file.
func (acl *ACL) SetFileAccess(path string) error {
	return acl.setFile(path, C.ACL_TYPE_ACCESS)
}

// SetFileDefault applies the default ACL to a file.
func (acl *ACL) SetFileDefault(path string) error {
	return acl.setFile(path, C.ACL_TYPE_DEFAULT)
}

// Free releases the memory used by the ACL.
func (acl *ACL) Free() {
	C.acl_free(unsafe.Pointer(acl.a))
}

// Parse constructs and ACL from a string representation.
func Parse(s string) (*ACL, error) {
	cs := C.CString(s)
	cacl, err := C.acl_from_text(cs)
	if cacl == nil {
		errno, _ := err.(syscall.Errno)
		return nil, fmt.Errorf("unable to parse ACL: %w", errno)
	}
	return &ACL{cacl}, nil
}

func getFile(path string, tp C.acl_type_t) (*ACL, error) {
	cacl, err := C.acl_get_file(C.CString(path), tp)
	if cacl == nil {
		errno, _ := err.(syscall.Errno)
		return nil, fmt.Errorf("unable to get ACL from file: %w", errno)
	}
	return &ACL{cacl}, nil
}

// GetFileAccess returns the access ACL associated with the given file path.
func GetFileAccess(path string) (*ACL, error) {
	return getFile(path, C.ACL_TYPE_ACCESS)
}

// GetFileDefault returns the default ACL associated with the given file path.
func GetFileDefault(path string) (*ACL, error) {
	return getFile(path, C.ACL_TYPE_DEFAULT)
}

func (acl *ACL) Size() int64 {
	return int64(C.acl_size_wrapper(acl.a))
}

func (acl *ACL) CopyExt(buffer []byte) (int64, error) {
	p := unsafe.Pointer(&buffer[0])
	l := C.ssize_t(len(buffer))
	i, err := C.acl_copy_ext(p, acl.a, l)
	if i < 0 {
		errno, _ := err.(syscall.Errno)
		return int64(i), fmt.Errorf("unable to copy ACL to external representation: %w", errno)
	}
	return int64(i), nil
}

func CopyInt(buffer []byte) (*ACL, error) {
	p := unsafe.Pointer(&buffer[0])
	cacl, err := C.acl_copy_int(p)
	if cacl == nil {
		errno, _ := err.(syscall.Errno)
		return nil, fmt.Errorf("unable to copy ACL to internal representation: %w", errno)
	}
	return &ACL{cacl}, nil
}

func (acl *ACL) EquivMode() (bool, os.FileMode, error) {
	var cmode C.mode_t
	var mode os.FileMode
	cequiv, err := C.acl_equiv_mode(acl.a, &cmode)
	if cequiv == -1 {
		errno, _ := err.(syscall.Errno)
		return false, mode, fmt.Errorf("unable to run acl_equiv_mode: %w", errno)
	}
	mode = os.FileMode(cmode) & os.ModePerm
	equiv := cequiv == 0
	return equiv, mode, nil
}
