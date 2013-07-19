#include "hyperleveldb/env.h"
#include "hyperleveldb/status.h"
#include "port/port.h"
#include "util/backup_env.h"
#include "db/filename.h"

namespace leveldb {
  Status BackupEnv::DeleteFile(const std::string& f) {
    mu_.Lock();
    Status s;
    if (backing_up_) {
      deferred_deletions_.push_back(f);
    } else {
      s = target()->DeleteFile(f);
    }
    mu_.Unlock();
    return s;
  }

  // Call (*save)(arg, filename, length) for every file that
  // should be backed up to construct a consistent view of the
  // database.  "length" may be negative to indicate that the entire
  // file must be copied.  Otherwise the first "length" bytes must be
  // copied.
  Status BackupEnv::Backup(const std::string& dir,
                int (*func)(void*, const char* fname, uint64_t length),
                void* arg) {
    // Get a consistent view of all files.
    mu_.Lock();
    std::vector<std::string> files;
    Status s = GetChildren(dir, &files);
    if (!s.ok()) {
      mu_.Unlock();
      return s;
    }
    std::vector<uint64_t> lengths(files.size());
    for (size_t i = 0; i < files.size(); i++) {
      if (files[i][0] == '.') {
        continue;
      }
      if (CopyFile(files[i])) {
        uint64_t len;
        s = GetFileSize(dir + "/" + files[i], &len);
        if (!s.ok()) {
          mu_.Unlock();
          return s;
        }
        lengths[i] = len;
      } else {
        lengths[i] = -1;
      }
    }
    backing_up_ = true;
    mu_.Unlock();

    for (size_t i = 0; s.ok() && i < files.size(); i++) {
      if (KeepFile(files[i])) {
        if ((*func)(arg, files[i].c_str(), lengths[i]) != 0) {
            s = Status::IOError("backup failed");
            break;
        }
      }
    }

    mu_.Lock();
    backing_up_ = false;
    for (size_t i = 0; i < deferred_deletions_.size(); i++) {
      target()->DeleteFile(deferred_deletions_[i]);
    }
    deferred_deletions_.clear();
    mu_.Unlock();

    return s;
  }

  bool BackupEnv::CopyFile(const std::string& fname) {
    uint64_t number;
    FileType type;
    ParseFileName(fname, &number, &type);
    return type != kTableFile;
  }

  bool BackupEnv::KeepFile(const std::string& fname) {
    uint64_t number;
    FileType type;
    if (ParseFileName(fname, &number, &type)) {
      switch (type) {
        case kLogFile:
        case kTableFile:
        case kDescriptorFile:
        case kCurrentFile:
        case kInfoLogFile:
          return true;
      }
    }
    return false;
  }
} // end namespace leveldb
