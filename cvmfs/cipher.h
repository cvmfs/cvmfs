/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CIPHER_H_
#define CVMFS_CIPHER_H_

#include <string>
#include <cstdio>

namespace cipher{

  bool LoadCertificatePath(const std::string &file_pem);

  bool LoadPrivateKeyPath(const std::string &file_pem, const std::string &password);

  bool LoadAESKey();

  bool LoadIV(const std::string &iv);

bool Decrypt(const unsigned char *buffer,
	     const unsigned buffer_size);

}


#endif
