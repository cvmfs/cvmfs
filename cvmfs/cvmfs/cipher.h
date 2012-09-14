/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CIPHER_H_
#define CVMFS_CIPHER_H_

#include <string>


namespace cipher{

void Init();

void Fini();

bool LoadCertificatePath(const std::string &file_pem);

bool LoadPrivateKeyPath(const std::string &file_pem, const std::string &password);

bool LoadAESKey();

bool LoadIV(const char *iv, unsigned ivlen);

int UnHexlify(char* str, unsigned len, char** buf);

int Decrypt(const unsigned char *buffer,
	    const unsigned buffer_size,
	    const unsigned char **ptr);

int unbase64(const char *input, int length, const char** output);
}




#endif
