#ifndef BACKUPENV_H
#define BACKUPENV_H

#include "hyperleveldb/env.h"
#include "port/port.h"

namespace leveldb {
class BackupEnv : public leveldb::EnvWrapper {
 private:
  port::Mutex mu_;
  bool backing_up_;
  std::vector<std::string> deferred_deletions_;

 public:
  explicit BackupEnv(leveldb::Env* t)
      : leveldb::EnvWrapper(t), backing_up_(false) {
  }

  Status DeleteFile(const std::string& f);

  // Call (*save)(arg, filename, length) for every file that
  // should be backed up to construct a consistent view of the
  // database.  "length" may be negative to indicate that the entire
  // file must be copied.  Otherwise the first "length" bytes must be
  // copied.
  Status Backup(const std::string& dir,
                int (*func)(void*, const char* fname, uint64_t length),
                void* arg);

  bool CopyFile(const std::string& fname);
  bool KeepFile(const std::string& fname);
};
} // end namespace leveldb

#endif
