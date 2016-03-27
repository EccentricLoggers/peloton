/*-------------------------------------------------------------------------
 *
 * wal_frontend_logger.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/wal_frontend_logger.h
 *
 *-------------------------------------------------------------------------
 */

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

  void TruncateLog(int);

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
};

}  // namespace logging
}  // namespace peloton
