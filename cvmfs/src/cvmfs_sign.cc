/*
 *  \file cvmfsd_sign.cc
 *  This tool signs a CernVM-FS file catalog and its nested catalogs
 *  with an X.509 certificate.
 *
 * Developed by Jakob Blomer at CERN, 2010
 */
 
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <set>
#include <vector>
#include <cstdlib>
#include <cstdio>
#include <sys/types.h>
#include <sys/stat.h>
#include <termios.h>
#include <dirent.h>
#include <unistd.h>

#include "signature.h"
#include "hash.h"
#include "util.h"

extern "C" {
   #include "compression.h"
   #include "smalloc.h"
}

using namespace std;

static void usage() {
   cout << "This tool signs a CernVM-FS file catalog." << endl;
   cout << "Usage: cvmfs_sign [-c <x509 certificate>] [-k <private key>] [-p <password>] [-n <repository name>] <catalog>" << endl;
}

int main(int argc, char **argv) {  
   if (argc < 2) {
      usage();
      return 1;
   }
   
   string dir_catalogs = "";
   string certificate = "";
   string priv_key = "";
   string pwd = "";
   string repo_name = "";
   
   char c;
   while ((c = getopt(argc, argv, "c:k:p:n:h")) != -1) {
      switch (c) {
         case 'c':
            certificate = optarg;
            break;
         case 'k':
            priv_key = optarg;
            break;
         case 'p':
            pwd = optarg;
            break;
         case 'n':
            repo_name = optarg;
            break;
         case 'h':
            usage();
            return 0;
         case '?':
         default:
            abort();
      }
   }
   if (optind >= argc) {
      usage();
      return 1;
   }
   dir_catalogs = canonical_path(get_parent_path(string(argv[optind])));
   
   const string clg_path = dir_catalogs + "/.cvmfscatalog.working";
   const string snapshot_path = dir_catalogs + "/.cvmfscatalog";
   
   /* Load certificate */
   signature::init();
   
   void *cert_buf;
   unsigned cert_buf_size;
   if (certificate == "") {
      cout << "Enter file name of X509 certificate []: " << flush;
      getline(cin, certificate);
   }
   if (!signature::load_certificate(certificate, false) ||
       !signature::write_certificate(&cert_buf, &cert_buf_size)) {
      cerr << "failed to load certificate" << endl;
      return 2;
   }
   //cout << signature::whois() << endl;
   
   /* Load private key */
   if (priv_key == "") {
      cout << "Enter file name of private key file to your certificate []: " << flush;
      getline(cin, priv_key);
   }
   if (!signature::load_private_key(priv_key, pwd)) {
      int retry = 0;
      bool success;
      do {
         struct termios defrsett, newrsett;
         char c;
         tcgetattr(fileno(stdin), &defrsett);
         newrsett = defrsett;
         newrsett.c_lflag &= ~ECHO;
         if(tcsetattr(fileno(stdin), TCSAFLUSH, &newrsett) != 0) {
            cerr << "terminal failure" << endl;
            return 2;
         }
      
         cout << "Enter password for private key: " << flush;
         pwd = "";
         while (cin.get(c) && (c != '\n'))
            pwd += c;
         tcsetattr(fileno(stdin), TCSANOW, &defrsett);
         cout << endl;
         
         success = signature::load_private_key(priv_key, pwd);
         if (!success)
            cerr << "failed to load private key, " << signature::get_crypto_err() << endl;
         retry++;
      } while (!success && (retry < 3));
      if (!success)
         return 2;
   }
   if (!signature::keys_match()) {
      cerr << "the private key doesn't seem to match your certificate " << signature::get_crypto_err() << endl;  
      signature::unload_private_key();
      return 2;
   }
   
   /* Now do the real work */
   {
      cout << "Signing " << snapshot_path << endl;
      const string chksum_path = dir_catalogs + "/.cvmfschecksum";
      
      ifstream chkfile(chksum_path.c_str(), ios::in|ios::binary|ios::ate);
      if (!chkfile.is_open()) {
         cerr << "Failed to open " << chksum_path << endl;
         goto sign_fail;
      }
      
      /* read .cvmfschecksum */
      ifstream::pos_type buf_compr_size = chkfile.tellg();
      void *buf_compr = smalloc(buf_compr_size);
      chkfile.seekg(0, ios::beg);
      chkfile.read((char *)buf_compr, buf_compr_size);
      chkfile.close();
      
      /* uncompress, extract sha1 + addons */
      void *sha1_buf;
      size_t sha1_buf_size;
      if ((decompress_mem(buf_compr, buf_compr_size, &sha1_buf, &sha1_buf_size) != 0) ||
          (sha1_buf_size < 40)) 
      {
         cerr << "Failed to read checksum" << endl;
         goto sign_fail;
      }
      hash::t_sha1 sha1;
      sha1.from_hash_str(string((char *)sha1_buf, 40));
      /* Preserve the checksum and the optional 'E<KEYGUID>' */
      string write_back = sha1.to_string();
      unsigned pos = 40;
      while ((pos < sha1_buf_size) && (((char *)sha1_buf)[pos] != '\n')) {
         write_back += ((char *)sha1_buf)[pos];
         pos++;
      }
      free(buf_compr);
      free(sha1_buf);
      
      /* Sign checksum */
      void *sig;
      unsigned sig_size;
      if (!signature::sign(sha1.to_string().c_str(), 40, &sig, &sig_size)) {
         cerr << "failed to sign" << endl;
         goto sign_fail;
      }
      
      /* Safe checksum and signature */
      char tail = '\n';
      unsigned chk_buf_size = write_back.length() + 1 + sig_size;
      char *chk_buf = (char *)smalloc(chk_buf_size);
      memcpy(chk_buf, write_back.c_str(), write_back.length());
      memcpy(chk_buf + write_back.length(), &tail, 1);
      memcpy(chk_buf + write_back.length() + 1, sig, sig_size);
      void *compr_buf;
      size_t compr_size;
      if (compress_mem(chk_buf, chk_buf_size, &compr_buf, &compr_size) != 0) {
         cerr << "Failed to compress signature" << endl;
         goto sign_fail;
      }
      free(sig);
      free(chk_buf);
      
      FILE *fp = fopen(chksum_path.c_str(), "w");
      if (fp == NULL) {
         cerr << "Failed to save signature" << endl;
         goto sign_fail;
      }
      if (fwrite(compr_buf, 1, compr_size, fp) < compr_size) {
         cerr << "Failed to save signature" << endl;
         goto sign_fail;
      }
      fclose(fp);
      free(compr_buf);
      
      /* Safe certificate */
      if (compress_mem(cert_buf, cert_buf_size, &compr_buf, &compr_size) != 0) {
         cerr << "Failed to compress certificate" << endl;
         goto sign_fail;
      }
      sha1_mem(compr_buf, compr_size, sha1.digest);
      const string cert_path_tmp = dir_catalogs + "/data/txn/cvmfspublisher.tmp";
      int fd_cert;
      FILE *fcert;
      if ( ((fd_cert = open(cert_path_tmp.c_str(), O_CREAT | O_TRUNC | O_RDWR, plain_file_mode)) < 0) ||
          !(fcert = fdopen(fd_cert, "w")) )
      {
         cerr << "Failed to save certificate" << endl;
         goto sign_fail;
      }
      if (fwrite(compr_buf, 1, compr_size, fcert) < compr_size) {
         cerr << "Failed to save certificate" << endl;
         goto sign_fail;
      }
      fclose(fcert);
      free(compr_buf);
      
      const string sha1_path = sha1.to_string().substr(0, 2) + "/" + 
      sha1.to_string().substr(2) + "X";
      const string cert_path = dir_catalogs + "/data/" + sha1_path;
      if (rename(cert_path_tmp.c_str(), cert_path.c_str()) != 0) {
         cerr << "Failed to store certificate in " << cert_path << endl;
         goto sign_fail;
      }
      unlink((dir_catalogs + "/.cvmfspublisher.x509").c_str());
      if (symlink(("data/" + sha1_path).c_str(), (dir_catalogs + "/.cvmfspublisher.x509").c_str()) != 0) {
         cerr << "Failed to symlink certificate" << endl;
         goto sign_fail;
      }
      
      /* Write extended checksum */
      map<char, string> content;
      if (!parse_keyval(dir_catalogs + "/.cvmfspublished", content) ||
          (content.find('C') == content.end()))
      {
         //cerr << "Failed to read extended checksum" << endl;
      } else {
         content['X'] = sha1.to_string();
         ostringstream str_time;
         str_time << time(NULL);
         content['T'] = str_time.str();
         if (repo_name != "")
            content['N'] = repo_name;
         
         string final;
         final = 'C' + content['C'] + "\n";
         for (map<char, string>::const_iterator itr = content.begin(), itrEnd = content.end();
              itr != itrEnd; ++itr)
         {
            if (itr->first == 'C')
               continue;
            final += itr->first + itr->second + "\n";
         }
         sha1_mem(&(final[0]), final.length(), sha1.digest);
         const string sha1_str = sha1.to_string();
         final += "--\n" + sha1_str + "\n";
         
         FILE *fext = fopen((dir_catalogs + "/.cvmfspublished").c_str(), "w");
         if (!fext) {
            cerr << "Failed to write extended checksum" << endl;
            goto sign_fail;
         }
         if (fwrite(&(final[0]), 1, final.length(), fext) != final.length()) {
            cerr << "Failed to write extended checksum" << endl;
            fclose(fext);
            goto sign_fail;
         }
         
         /* Sign checksum and write signature */
         void *sig;
         unsigned sig_size;
         if (!signature::sign(&(sha1_str[0]), 40, &sig, &sig_size)) {
            cerr << "Failed to sign extended checksum" << endl;
            fclose(fext);
            goto sign_fail;
         }
         if (fwrite(sig, 1, sig_size, fext) != sig_size) {
            cerr << "Failed to write checksum for extended checksum" << endl;
            free(sig);
            fclose(fext);
            goto sign_fail;
         }
         free(sig);
         
         fclose(fext);
      }
   }
   
   signature::fini();
   return 0;
sign_fail:
   signature::fini();
   return 1;
}

