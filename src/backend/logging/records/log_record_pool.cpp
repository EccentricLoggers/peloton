#include "log_record_pool.h"

namespace peloton {
namespace logging {

void LogRecordList::Clear() {
  // Clean up
  LogRecordNode *cur = head_node;
  while (cur != nullptr) {
    LogRecordNode *deletingNode = cur;
    cur = cur->next_node;
    head_node = cur;
    _backend->Free(deletingNode);
  }
  tail_node = nullptr;
}

int LogRecordList::AddLogRecord(TupleRecord *record) {
  if (_backend == nullptr || record == nullptr || record->GetTransactionId() != txn_id)
    return -1;

  LogRecordNode *localRecord = (LogRecordNode*) _backend->Allocate(sizeof(LogRecordNode));
  if (localRecord != nullptr) {
    localRecord->_db_oid = record->GetDatabaseOid();
    localRecord->_delete_location = record->GetDeleteLocation();
    localRecord->_insert_location = record->GetInsertLocation();
    localRecord->_log_record_type = record->GetType();
    localRecord->_table_oid = record->GetTableId();
    localRecord->next_node = nullptr;

    // add to the list
    if (tail_node == nullptr) {
      head_node = localRecord;
    } else {
      tail_node->next_node = localRecord;
    }
    tail_node = localRecord;
    return 0;
  } else {
    return -1;
  }
}

void LogRecordPool::Clear() {
  // Clean up
  LogRecordList *cur = head_list;
  while (cur != nullptr) {
    LogRecordList *deletingNode = cur;
    cur = cur->GetNextList();
    RemoveLogList(nullptr, deletingNode);
  }
  tail_list = nullptr;
}

int LogRecordPool::CreateTxnLogList(txn_id_t txn_id) {
  if (_backend == nullptr) {
    return -1;
  }
  LogRecordList* existing_list = SearchRecordList(txn_id);
  if (existing_list == nullptr) {
    existing_list = (LogRecordList*) _backend->Allocate(sizeof(LogRecordList));
    if (existing_list == nullptr) {
      return -1;
    }
    existing_list->init(_backend);
    existing_list->SetTxnId(txn_id);

    // add to the pool
    if (tail_list == nullptr) {
      head_list = existing_list;
    } else {
      tail_list->SetNextList(existing_list);
    }
    tail_list = existing_list;
  }
  return 0;
}

int LogRecordPool::AddLogRecord(TupleRecord *record) {
  LogRecordList* existing_list = SearchRecordList(record->GetTransactionId());
  if (existing_list != nullptr) {
    return existing_list->AddLogRecord(record);
  }
  return -1;
}

void LogRecordPool::RemoveTxnLogList(txn_id_t txn_id) {
  LogRecordList *prev = nullptr;
  LogRecordList *cur = head_list;
  while (cur != nullptr) {
    if (cur->GetTxnID() == txn_id) {
      RemoveLogList(prev, cur);
      break;
    }
    prev = cur;
    cur = cur->GetNextList();
  }
}

LogRecordList* LogRecordPool::SearchRecordList(txn_id_t txn_id) {
  LogRecordList *cur = head_list;
  while (cur != nullptr) {
    if (cur->GetTxnID() == txn_id) {
      return cur;
    }
    cur = cur->GetNextList();
  }
  return nullptr;
}

void LogRecordPool::RemoveLogList(LogRecordList *prev, LogRecordList *list) {
  if (prev == nullptr) {
    head_list = list->GetNextList();
  } else {
    prev->SetNextList(list->GetNextList());
  }
  if (list->GetNextList() == nullptr) {
    tail_list = prev;
  }
  // clean the list record
  list->Clear();
  _backend->Free(list);
}
}
}