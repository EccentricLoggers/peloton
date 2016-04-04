//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// frontend_logger.h
//
// Identification: src/backend/logging/frontend_logger.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <iostream>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <unistd.h>

#include "backend/common/types.h"
#include "backend/logging/logger.h"
#include "backend/logging/backend_logger.h"
#include "backend/logging/checkpoint.h"
#include "backend/logging/records/tuple_record.h"

namespace peloton {
namespace logging {

class Checkpoint;

//===--------------------------------------------------------------------===//
// Frontend Logger
//===--------------------------------------------------------------------===//

class FrontendLogger : public Logger {
 public:
  FrontendLogger();

  ~FrontendLogger();

  static FrontendLogger *GetFrontendLogger(LoggingType logging_type);

  void MainLoop(void);

  void CollectLogRecordsFromBackendLoggers(void);

  void AddBackendLogger(BackendLogger *backend_logger);

  bool RemoveBackendLogger(BackendLogger *backend_logger);

  //===--------------------------------------------------------------------===//
  // Virtual Functions
  //===--------------------------------------------------------------------===//

  // Flush collected LogRecords
  virtual void FlushLogRecords(void) = 0;

  // Restore database
  virtual void DoRecovery(void) = 0;

  size_t GetFsyncCount() const { return fsync_count; }

  void ReplayLog(const char *, size_t len);

  void StartTransactionRecovery(cid_t commit_id);

  void CommitTransactionRecovery(cid_t commit_id);

  void InsertTuple(TupleRecord *recovery_txn);

  void DeleteTuple(TupleRecord *recovery_txn);

  void UpdateTuple(TupleRecord *recovery_txn);

  void AddTupleRecord(cid_t commit_id, TupleRecord* tuple_record);

  VarlenPool *GetRecoveryPool(void);

 protected:
  // Associated backend loggers
  std::vector<BackendLogger *> backend_loggers;

  // Since backend loggers can add themselves into the list above
  // via log manager, we need to protect the backend_loggers list
  std::mutex backend_logger_mutex;

  // Global queue
  std::vector<LogRecord *> global_queue;

  // period with which it collects log records from backend loggers
  // (in microseconds)
  int64_t wait_timeout;

  // used to indicate if backend has new logs
  bool need_to_collect_new_log_records = false;

  // stats
  size_t fsync_count = 0;

  // checkpoint
  Checkpoint &checkpoint;
  // Txn table during recovery
  std::map<txn_id_t, std::vector<TupleRecord *>> recovery_txn_table;

  // Keep tracking max oid for setting next_oid in manager
  // For active processing after recovery
  oid_t max_oid = 0;

  cid_t max_cid = 0;

  // pool for allocating non-inlined values
  VarlenPool *recovery_pool = new VarlenPool(BACKEND_TYPE_MM);
};

}  // namespace logging
}  // namespace peloton
