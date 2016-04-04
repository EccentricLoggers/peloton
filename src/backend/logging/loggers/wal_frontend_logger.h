//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// wal_frontend_logger.h
//
// Identification: src/backend/logging/loggers/wal_frontend_logger.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/logging/frontend_logger.h"
#include "backend/logging/records/tuple_record.h"
#include "backend/logging/log_file.h"
#include "backend/networking/rpc_channel.h"
#include "backend/networking/rpc_controller.h"
#include <dirent.h>

namespace peloton {

class VarlenPool;

namespace concurrency {
class Transaction;
}

namespace logging {

//===--------------------------------------------------------------------===//
// Write Ahead Frontend Logger
//===--------------------------------------------------------------------===//

class WriteAheadFrontendLogger : public FrontendLogger {
 public:
  WriteAheadFrontendLogger(void);

  WriteAheadFrontendLogger(bool for_testing);

  ~WriteAheadFrontendLogger(void);

  void FlushLogRecords(void);

  //===--------------------------------------------------------------------===//
  // Recovery
  //===--------------------------------------------------------------------===//

  void DoRecovery(void);

  void AbortActiveTransactions();

  void InitLogFilesList();

  void CreateNewLogFile(bool);

  bool FileSwitchCondIsTrue();

  void OpenNextLogFile();

  LogRecordType GetNextLogRecordTypeForRecovery(FILE *, size_t);

  void TruncateLog(txn_id_t);

  void SetLogDirectory(char *);

  void InitLogDirectory();

  std::string GetFileNameFromVersion(int);

  txn_id_t ExtractMaxCommitIdFromLogFileRecords(FILE *);

 private:
  std::string GetLogFileName(void);

  //===--------------------------------------------------------------------===//
  // Member Variables
  //===--------------------------------------------------------------------===//

  // File pointer and descriptor
  FILE *log_file;
  int log_file_fd;

  // Size of the log file
  size_t log_file_size;

  // pool for allocating non-inlined values
  VarlenPool *recovery_pool;

  // abj1 adding code here!
  std::vector<LogFile *> log_files_;

  int log_file_counter_;

  int log_file_cursor_;

  networking::RpcChannel *channel_ =  new networking::RpcChannel("127.0.0.1:9000");
  networking::RpcController *controller_ = new networking::RpcController();
  //for recovery from in memory buffer instead of file.
  char * input_log_buffer;

  std::string peloton_log_directory = "peloton_log";

  std::string LOG_FILE_PREFIX = "peloton_log_";

  std::string LOG_FILE_SUFFIX = ".log";

  txn_id_t max_commit_id;
};

}  // namespace logging
}  // namespace peloton
