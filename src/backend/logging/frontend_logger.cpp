//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// frontend_logger.cpp
//
// Identification: src/backend/logging/frontend_logger.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <thread>

#include "backend/common/logger.h"
#include "backend/logging/log_manager.h"
#include "backend/logging/checkpoint_manager.h"
#include "backend/logging/frontend_logger.h"
#include "backend/logging/checkpoint.h"
#include "backend/logging/loggers/wal_frontend_logger.h"
#include "backend/logging/loggers/wbl_frontend_logger.h"
#include "backend/catalog/manager.h"
#include "backend/catalog/schema.h"
#include "backend/storage/database.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tuple.h"
#include "backend/concurrency/transaction_manager_factory.h"

// configuration for testing
extern int64_t peloton_wait_timeout;

namespace peloton {
namespace logging {

FrontendLogger::FrontendLogger() {
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
 * @param logging type can be write ahead logging or write behind logging
 */
FrontendLogger *FrontendLogger::GetFrontendLogger(LoggingType logging_type,
                                                  bool test_mode) {
  FrontendLogger *frontend_logger = nullptr;

  if (IsBasedOnWriteAheadLogging(logging_type) == true) {
    frontend_logger = new WriteAheadFrontendLogger(test_mode);
  } else if (IsBasedOnWriteBehindLogging(logging_type) == true) {
    frontend_logger = new WriteBehindFrontendLogger();
  } else {
    LOG_ERROR("Unsupported logging type");
  }

  return frontend_logger;
}

/**
 * @brief MainLoop
 */
void FrontendLogger::MainLoop(void) {
  auto &log_manager = LogManager::GetInstance();

  /////////////////////////////////////////////////////////////////////
  // STANDBY MODE
  /////////////////////////////////////////////////////////////////////

  LOG_TRACE("FrontendLogger Standby Mode");

  // Standby before we need to do RECOVERY
  log_manager.WaitForModeTransition(LOGGING_STATUS_TYPE_STANDBY, false);

  // Do recovery if we can, otherwise terminate
  switch (log_manager.GetLoggingStatus()) {
    case LOGGING_STATUS_TYPE_RECOVERY: {
      LOG_TRACE("Frontendlogger] Recovery Mode");

      /////////////////////////////////////////////////////////////////////
      // RECOVERY MODE
      /////////////////////////////////////////////////////////////////////

      // First, do recovery if needed
      LOG_INFO("Log manager: Invoking DoRecovery");
      DoRecovery();
      LOG_INFO("Log manager: DoRecovery done");

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
  while (log_manager.GetLoggingStatus() == LOGGING_STATUS_TYPE_LOGGING) {
    // Collect LogRecords from all backend loggers
    // LOG_INFO("Log manager: Invoking CollectLogRecordsFromBackendLoggers");
    CollectLogRecordsFromBackendLoggers();

    // Flush the data to the file
    // LOG_INFO("Log manager: Invoking FlushLogRecords");
    FlushLogRecords();
  }

  /////////////////////////////////////////////////////////////////////
  // TERMINATE MODE
  /////////////////////////////////////////////////////////////////////

  // flush any remaining log records
  CollectLogRecordsFromBackendLoggers();
  FlushLogRecords();

  /////////////////////////////////////////////////////////////////////
  // SLEEP MODE
  /////////////////////////////////////////////////////////////////////

  LOG_TRACE("Frontendlogger Sleep Mode");

  // Setting frontend logger status to sleep
  log_manager.SetLoggingStatus(LOGGING_STATUS_TYPE_SLEEP);
}

/**
 * @brief Collect the log records from BackendLoggers
 */
void FrontendLogger::CollectLogRecordsFromBackendLoggers() {
  auto sleep_period = std::chrono::milliseconds(wait_timeout);
  std::this_thread::sleep_for(sleep_period);

  {
    cid_t max_committed_cid = 0;
    cid_t lower_bound = MAX_CID;

    // Look at the local queues of the backend loggers
    backend_loggers_lock.Lock();
    LOG_TRACE("Collect log buffers from %lu backend loggers",
              backend_loggers.size());
    int i = 0;
    for (auto backend_logger : backend_loggers) {
      auto cid_pair = backend_logger->PrepareLogBuffers();
      auto &log_buffers = backend_logger->GetLogBuffers();
      auto log_buffer_size = log_buffers.size();

      // update max_possible_commit_id with the latest buffer
      cid_t backend_lower_bound = cid_pair.first;
      cid_t backend_committed = cid_pair.second;
      if (backend_committed > backend_lower_bound) {
        LOG_INFO("bel: %d got max_committed_cid:%lu", i, backend_committed);
        max_committed_cid = std::max(max_committed_cid, backend_committed);
      } else if (backend_lower_bound != INVALID_CID) {
        LOG_INFO("bel: %d got lower_bound_cid:%lu", i, backend_lower_bound);
        lower_bound = std::min(lower_bound, backend_lower_bound);
      }
      // Skip current backend_logger, nothing to do
      if (log_buffer_size == 0) {
        i++;
        continue;
      }

      // Move the log record from backend_logger to here
      for (oid_t log_record_itr = 0; log_record_itr < log_buffer_size;
           log_record_itr++) {
        // copy to front end logger
        global_queue.push_back(std::move(log_buffers[log_record_itr]));
      }

      // cleanup the local queue
      log_buffers.clear();
      i++;
    }
    cid_t max_possible_commit_id;
    if (max_committed_cid == 0 && lower_bound == MAX_CID) {
      // nothing collected
      max_possible_commit_id = max_collected_commit_id;
    } else if (max_committed_cid == 0) {
      max_possible_commit_id = lower_bound;
    } else if (lower_bound == MAX_CID) {
      max_possible_commit_id = max_committed_cid;
    } else {
      max_possible_commit_id = lower_bound;
    }
    // max_collected_commit_id should never decrease
    // LOG_INFO("Before assert");
    assert(max_possible_commit_id >= max_collected_commit_id);
    if (max_committed_cid > max_seen_commit_id) {
      max_seen_commit_id = max_committed_cid;
    }
    max_collected_commit_id = max_possible_commit_id;
    LOG_TRACE("max_collected_commit_id: %d", (int)max_collected_commit_id);
    backend_loggers_lock.Unlock();
  }
}

cid_t FrontendLogger::GetMaxFlushedCommitId() { return max_flushed_commit_id; }

void FrontendLogger::SetMaxFlushedCommitId(cid_t cid) {
  max_flushed_commit_id = cid;
}

void FrontendLogger::SetBackendLoggerLoggedCid(BackendLogger &bel) {
  backend_loggers_lock.Lock();
  bel.SetLoggingCidLowerBound(max_seen_commit_id);
  backend_loggers_lock.Unlock();
}

/**
 * @brief Add backend logger to the list of backend loggers
 * @param backend logger
 */
void FrontendLogger::AddBackendLogger(BackendLogger *backend_logger) {
  // Grant empty buffers
  for (int i = 0; i < BUFFER_POOL_SIZE; i++) {
    std::unique_ptr<LogBuffer> buffer(new LogBuffer(backend_logger));
    backend_logger->GrantEmptyBuffer(std::move(buffer));
  }
  // Add backend logger to the list of backend loggers
  backend_loggers_lock.Lock();
  backend_logger->SetLoggingCidLowerBound(max_collected_commit_id);
  backend_loggers.push_back(backend_logger);
  backend_loggers_lock.Unlock();
}

/**
 * @brief Add new txn to recovery table
 */
void FrontendLogger::StartTransactionRecovery(cid_t commit_id) {
  std::vector<TupleRecord *> tuple_recs;
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
    if (concurrency::TransactionManagerFactory::GetInstance().GetNextCommitId() <= commit_id){
	concurrency::TransactionManagerFactory::GetInstance().SetNextCid(commit_id+1);
    }
    delete curr;
  }
  max_cid = commit_id + 1;
  recovery_txn_table.erase(commit_id);
}

void InsertTupleHelper(oid_t &max_tg, cid_t commit_id, oid_t db_id, oid_t table_id, const ItemPointer &insert_loc, storage::Tuple* tuple, bool increase_count = true){
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
  if (increase_count){
      table->IncreaseNumberOfTuplesBy(1);
  }
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
  table->DecreaseNumberOfTuplesBy(1);
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
  InsertTupleHelper(max_tg, commit_id, db_id, table_id, insert_loc, tuple, false);

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
