/*-------------------------------------------------------------------------
 *
 * frontendlogger.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/frontendlogger.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <thread>

#include "backend/common/logger.h"
#include "backend/logging/log_manager.h"
#include "backend/logging/frontend_logger.h"
#include "backend/logging/checkpoint.h"
#include "backend/logging/checkpoint_factory.h"
#include "backend/logging/loggers/wal_frontend_logger.h"
#include "backend/logging/loggers/wbl_frontend_logger.h"
#include "backend/catalog/manager.h"
#include "backend/catalog/schema.h"
#include "backend/storage/database.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tuple.h"

// configuration for testing
extern int64_t peloton_wait_timeout;

namespace peloton {
namespace logging {

FrontendLogger::FrontendLogger() : checkpoint(CheckpointFactory::GetInstance()) {
  logger_type = LOGGER_TYPE_FRONTEND;

  // Set wait timeout
  wait_timeout = peloton_wait_timeout;
}

FrontendLogger::~FrontendLogger() {
  for (auto backend_logger : backend_loggers) {
    delete backend_logger;
  }
  delete recovery_pool;
}

/** * @brief Return the frontend logger based on logging type
 * @param logging type can be stdout(debug), aries, peloton
 */
FrontendLogger *FrontendLogger::GetFrontendLogger(LoggingType logging_type) {
  FrontendLogger *frontendLogger = nullptr;

  if (IsBasedOnWriteAheadLogging(logging_type) == true) {
    frontendLogger = new WriteAheadFrontendLogger();
  } else if (IsBasedOnWriteBehindLogging(logging_type) == true) {
    frontendLogger = new WriteBehindFrontendLogger();
  } else {
    LOG_ERROR("Unsupported logging type");
  }

  return frontendLogger;
}

/**
 * @brief MainLoop
 */
void FrontendLogger::MainLoop(void) {
  auto &log_manager = LogManager::GetInstance();

  /////////////////////////////////////////////////////////////////////
  // STANDBY MODE
  /////////////////////////////////////////////////////////////////////

  LOG_TRACE("Frontendlogger] Standby Mode");

  // Standby before we need to do RECOVERY
  log_manager.WaitForMode(LOGGING_STATUS_TYPE_STANDBY, false);

  // Do recovery if we can, otherwise terminate
  switch (log_manager.GetStatus()) {
    case LOGGING_STATUS_TYPE_RECOVERY: {
      LOG_TRACE("Frontendlogger] Recovery Mode");

      /////////////////////////////////////////////////////////////////////
      // RECOVERY MODE
      /////////////////////////////////////////////////////////////////////

      // First, do recovery if needed
      DoRecovery();

      // Now, enter LOGGING mode
      log_manager.SetLoggingStatus(LOGGING_STATUS_TYPE_LOGGING);

      break;
    }

    case LOGGING_STATUS_TYPE_LOGGING: {
      LOG_TRACE("Frontendlogger] Logging Mode");
    } break;

    default:
      break;
  }

  /////////////////////////////////////////////////////////////////////
  // LOGGING MODE
  /////////////////////////////////////////////////////////////////////

  // Periodically, wake up and do logging
  while (log_manager.GetStatus() == LOGGING_STATUS_TYPE_LOGGING) {
    // Collect LogRecords from all backend loggers
    CollectLogRecordsFromBackendLoggers();

    // Flush the data to the file
    FlushLogRecords();
  }

  /////////////////////////////////////////////////////////////////////
  // TERMINATE MODE
  /////////////////////////////////////////////////////////////////////

  // force the last check to be done without waiting
  need_to_collect_new_log_records = true;

  // flush any remaining log records
  CollectLogRecordsFromBackendLoggers();
  FlushLogRecords();

  /////////////////////////////////////////////////////////////////////
  // SLEEP MODE
  /////////////////////////////////////////////////////////////////////

  LOG_TRACE("Frontendlogger] Sleep Mode");

  // Setting frontend logger status to sleep
  log_manager.SetLoggingStatus(LOGGING_STATUS_TYPE_SLEEP);
}

/**
 * @brief Collect the log records from BackendLoggers
 */
void FrontendLogger::CollectLogRecordsFromBackendLoggers() {
  /*
   * Don't use "while(!need_to_collect_new_log_records)",
   * we want the frontend check all backend periodically even no backend
   * notifies.
   * So that large txn can submit its log records piece by piece
   * instead of a huge submission when the txn is committed.
   */
  if (need_to_collect_new_log_records == false) {
    auto sleep_period = std::chrono::microseconds(wait_timeout);
    std::this_thread::sleep_for(sleep_period);
  }

  {
    std::lock_guard<std::mutex> lock(backend_logger_mutex);

    // Look at the local queues of the backend loggers
    for (auto backend_logger : backend_loggers) {
      auto local_queue_size = backend_logger->GetLocalQueueSize();

      // Skip current backend_logger, nothing to do
      if (local_queue_size == 0) continue;

      // Shallow copy the log record from backend_logger to here
      for (oid_t log_record_itr = 0; log_record_itr < local_queue_size;
           log_record_itr++) {
        global_queue.push_back(backend_logger->GetLogRecord(log_record_itr));
      }

      // truncate the local queue
      backend_logger->TruncateLocalQueue(local_queue_size);
    }
  }

  need_to_collect_new_log_records = false;
}

/**
 * @brief Store backend logger
 * @param backend logger
 */
void FrontendLogger::AddBackendLogger(BackendLogger *backend_logger) {
  {
    std::lock_guard<std::mutex> lock(backend_logger_mutex);
    backend_loggers.push_back(backend_logger);
    backend_logger->SetConnectedToFrontend(true);
  }
}

bool FrontendLogger::RemoveBackendLogger(BackendLogger *_backend_logger) {
  {
    std::lock_guard<std::mutex> lock(backend_logger_mutex);
    oid_t offset = 0;

    for (auto backend_logger : backend_loggers) {
      if (backend_logger == _backend_logger) {
        backend_logger->SetConnectedToFrontend(false);
        break;
      } else {
        offset++;
      }
    }

    assert(offset < backend_loggers.size());
    backend_loggers.erase(backend_loggers.begin() + offset);
  }

  return true;
}

/**
 * @brief Add new txn to recovery table
 */
void FrontendLogger::StartTransactionRecovery(cid_t commit_id) {
  std::vector<TupleRecord*> tuple_recs;
  recovery_txn_table[commit_id] = tuple_recs;
}

/**
 * @brief move tuples from current txn to recovery txn so that we can commit
 * them later
 * @param recovery txn
 */
void FrontendLogger::CommitTransactionRecovery(cid_t commit_id) {
  std::vector<TupleRecord*> &tuple_records = recovery_txn_table[commit_id];
  for(auto it = tuple_records.begin(); it != tuple_records.end(); it++){
    TupleRecord* curr = *it;
    switch(curr->GetType()){
      case LOGRECORD_TYPE_WAL_TUPLE_INSERT:
	InsertTuple(curr);
	break;
      case LOGRECORD_TYPE_WAL_TUPLE_UPDATE:
	UpdateTuple(curr);
	break;
      case LOGRECORD_TYPE_WAL_TUPLE_DELETE:
	DeleteTuple(curr);
	break;
      default:
	continue;
    }
    delete curr;
  }
  max_cid = commit_id + 1;
  recovery_txn_table.erase(commit_id);
}

void InsertTupleHelper(oid_t &max_tg, cid_t commit_id, oid_t db_id, oid_t table_id, const ItemPointer &insert_loc, storage::Tuple* tuple){
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(insert_loc.block);
  storage::Database *db =
        manager.GetDatabaseWithOid(db_id);
  assert(db);

  auto table = db->GetTableWithOid(table_id);
  assert(table);
  if (tile_group == nullptr) {
    table->AddTileGroupWithOid(insert_loc.block);
    tile_group = manager.GetTileGroup(insert_loc.block);
    if (max_tg < insert_loc.block) {
	max_tg = insert_loc.block;
    }
  }

  tile_group->InsertTupleFromRecovery(commit_id, insert_loc.offset, tuple);
  delete tuple;
}

void DeleteTupleHelper(oid_t &max_tg, cid_t commit_id, oid_t db_id, oid_t table_id, const ItemPointer &delete_loc){
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(delete_loc.block);
  storage::Database *db =
	manager.GetDatabaseWithOid(db_id);
  assert(db);

  auto table = db->GetTableWithOid(table_id);
  assert(table);
  if (tile_group == nullptr) {
    table->AddTileGroupWithOid(delete_loc.block);
    tile_group = manager.GetTileGroup(delete_loc.block);
    if (max_tg < delete_loc.block) {
      max_tg = delete_loc.block;
    }
  }

  tile_group->DeleteTupleFromRecovery(commit_id, delete_loc.offset);
}

void UpdateTupleHelper(oid_t &max_tg, cid_t commit_id, oid_t db_id, oid_t table_id, const ItemPointer &remove_loc,const ItemPointer &insert_loc, storage::Tuple *tuple){
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(remove_loc.block);
  storage::Database *db =
	manager.GetDatabaseWithOid(db_id);
  assert(db);

  auto table = db->GetTableWithOid(table_id);
  assert(table);
  if (tile_group == nullptr) {
    table->AddTileGroupWithOid(remove_loc.block);
    tile_group = manager.GetTileGroup(remove_loc.block);
    if (max_tg < remove_loc.block) {
      max_tg = remove_loc.block;
    }
  }
  InsertTupleHelper(max_tg, commit_id, db_id, table_id, insert_loc, tuple);

  tile_group->UpdateTupleFromRecovery(commit_id, remove_loc.offset, insert_loc);
}

/**
 * @brief read tuple record from log file and add them tuples to recovery txn
 * @param recovery txn
 */
void FrontendLogger::InsertTuple(
    TupleRecord *record) {
  InsertTupleHelper(max_oid, record->GetTransactionId(), record->GetDatabaseOid(), record->GetTableId(), record->GetInsertLocation(), record->GetTuple());
}

/**
 * @brief read tuple record from log file and add them tuples to recovery txn
 * @param recovery txn
 */
void FrontendLogger::DeleteTuple(TupleRecord *record) {
  DeleteTupleHelper(max_oid, record->GetTransactionId(), record->GetDatabaseOid(), record->GetTableId(), record->GetDeleteLocation());
}

/**
 * @brief read tuple record from log file and add them tuples to recovery txn
 * @param recovery txn
 */

void FrontendLogger::UpdateTuple(
    TupleRecord *record) {
  UpdateTupleHelper(max_oid, record->GetTransactionId(), record->GetDatabaseOid(), record->GetTableId(), record->GetDeleteLocation(), record->GetInsertLocation(), record->GetTuple());
}

void FrontendLogger::AddTupleRecord(cid_t commit_id, TupleRecord* tuple_record){
  recovery_txn_table[commit_id].push_back(tuple_record);
}
VarlenPool * FrontendLogger::GetRecoveryPool(){
  return recovery_pool;
}

}  // namespace logging
}
