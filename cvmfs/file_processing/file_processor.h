/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_FILE_PROCESSING_FILE_PROCESSOR_H_
#define CVMFS_FILE_PROCESSING_FILE_PROCESSOR_H_

#include <string>

#include "../hash.h"
#include "../upload_spooler_result.h"
#include "../util.h"
#include "../util_concurrency.h"

namespace upload {

/**
 * Each orinary file in a CVMFS repository needs to be processed before storing
 * it for distribution. The FileProcessor as well as it's helper classes take
 * care of this process in a parallel fashing using TBB.
 *
 * The processing involves the following:
 *      -> create smaller file chunks for big input files
 *      -> compress the file content (optionally chunked)
 *      -> generate a content hash of the compression result
 *
 * The main components of the processing facility are:
 *  -> IoDispatcher
 *       Takes care of reading files from disk and scheduling TBB tasks to pro-
 *       cess them. As well it takes care of handing processed data to an
 *       Uploader provided by the outside.
 *       The IoDispatcher is supposed to be the synchronization point of the
 *       file processing pipeline as well as the central I/O handler.
 *  -> Processor
 *       The processor is a collection of TBB tasks that do the pure data pro-
 *       cessing. It crunches streamed data provided by the IoDispatcher and is
 *       not supposed to do any I/O in order to maximize it's throughput.
 *
 * The general idea of the processing pipe line is to decouple I/O from fast
 * computational work. Files are read by the IoDispatcher in small blocks that
 * are stored in CharBuffers and forwarded to the Processor and it's TBB tasks.
 * The TBB tasks process these blocks as a data stream and strive for maximal
 * concurrency in processing independent files.
 *
 * Important Terminology:
 *  -> Block
 *       Files are read in small data pieces by the IoDispatcher and processed
 *       in TBB tasks. We see this as an effective way of stream processing big
 *       files concurrently. (each Block is represented by a CharBuffer).
 *       Note: A Block is not related to a Chunk!
 *  -> Chunk
 *       Big files (usually bigger than 4 to 16 MiB) are sliced into smaller
 *       Chunks in order to cope with network related shortcomings. They are
 *       on the fly by the processing pipe line and get stored as individual
 *       objects in the backend storage.
 *  -> Bulk Chunk
 *       In order to keep backward compatibility, big files are stored both as
 *       one huge blob of data and as sliced Chunks as described before.
 *       Therefore big files need to be effectively processed twice.
 */


class AbstractUploader;
class IoDispatcher;
class File;
struct SpoolerDefinition;

/**
 * This is the outer most wrapper class that should be used by the Spooler.
 * It initializes the file processing pipe line and takes care of scheduling
 * and synchronization of file processing jobs.
 * Note: FileProcessor implements the Observable template and emits callbacks
 *       when files are successfully processed.
 */
class FileProcessor : public Observable<SpoolerResult> {
 public:
  FileProcessor(AbstractUploader         *uploader,
                const SpoolerDefinition  &spooler_definition);
  virtual ~FileProcessor();

  void Process(const std::string   &local_path,
               const bool           allow_chunking,
               const shash::Suffix  hash_suffix = shash::kSuffixNone);

  void WaitForProcessing();

 protected:
  friend class IoDispatcher;
  void FileDone(File *file);

 private:
  IoDispatcher  *io_dispatcher_;

  zlib::Algorithms   compression_alg_;
  shash::Algorithms  hash_algorithm_;
  const bool         chunking_enabled_;
  const size_t       minimal_chunk_size_;
  const size_t       average_chunk_size_;
  const size_t       maximal_chunk_size_;
};

}  // namespace upload

#endif  // CVMFS_FILE_PROCESSING_FILE_PROCESSOR_H_
