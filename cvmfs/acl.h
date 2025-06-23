/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_ACL_H_
#define CVMFS_ACL_H_

#include <inttypes.h>
#include <stddef.h>

#include <string>

/* Takes textual ACL, outputs binary array fit to be a value of
 * system.posix_acl_access */
int acl_from_text_to_xattr_value(const std::string &textual_acl,
                                 char *&o_binary_acl, size_t &o_size,
                                 bool &o_equiv_mode);

// #define COMPARE_TO_LIBACL
#ifdef COMPARE_TO_LIBACL
int acl_from_text_to_xattr_value_both_impl(const std::string textual_acl,
                                           char *&o_binary_acl, size_t &o_size,
                                           bool &o_equiv_mode);
#endif  // COMPARE_TO_LIBACL

#endif  // CVMFS_ACL_H_
