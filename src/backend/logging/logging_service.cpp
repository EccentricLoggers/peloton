//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// pelton_service.h
//
// Identification: src/backend/networking/pelton_service.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "logging_service.h"
#include "log_manager.h"
#include "backend/catalog/manager.h"
#include "backend/catalog/schema.h"
#include "backend/logging/records/transaction_record.h"
#include "backend/logging/records/tuple_record.h"
#include "backend/storage/database.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tuple.h"


#include <iostream>

namespace peloton {
namespace logging {

LogRecordType GetRecordTypeFromBytes(char *&workingPointer){
  CopySerializeInputBE input(workingPointer, sizeof(char));
  LogRecordType log_record_type = (LogRecordType)(input.ReadEnumInSingleByte());
  workingPointer++;
  return log_record_type;
}

size_t GetHeaderLen(char *&workingPointer){
  size_t frame_size;

  // Read next 4 bytes as an integer
  CopySerializeInputBE frameCheck(workingPointer, sizeof(int32_t));
  frame_size = (frameCheck.ReadInt()) + sizeof(int32_t);
  return frame_size;
}

void GetTransactionRecord(TransactionRecord &record, char *&workingPointer){
  auto frame_size = GetHeaderLen(workingPointer);


  CopySerializeInputBE txn_header(workingPointer, frame_size);
  record.Deserialize(txn_header);
  workingPointer +=frame_size;
}

void GetTupleRecordHeader(TupleRecord &record, char *&workingPointer){
  auto frame_size = GetHeaderLen(workingPointer);


  CopySerializeInputBE txn_header(workingPointer, frame_size);
  record.DeserializeHeader(txn_header);
  workingPointer +=frame_size;
}

void GetTupleRecord(TupleRecord &record, char *&workingPointer, VarlenPool * pool){
  GetTupleRecordHeader(record, workingPointer);
  auto frame_size = GetHeaderLen(workingPointer);

  auto schema = catalog::Manager::GetInstance().GetDatabaseWithOid(record.GetDatabaseOid())->
      GetTableWithOid(record.GetTableId())->GetSchema();
  CopySerializeInputBE tuple_body(workingPointer, frame_size);

  storage::Tuple *tuple = new storage::Tuple(schema, true);
  tuple->DeserializeFrom(tuple_body, pool);
  record.SetTuple(tuple);
}
// implements LoggingService ------------------------------------------

void LoggingService::LogRecordReplay( __attribute__((unused)) ::google::protobuf::RpcController* controller,
	const networking::LogRecordReplayRequest* request,
	networking::LogRecordReplayResponse* response,
	__attribute__((unused)) ::google::protobuf::Closure* done){
//  LogManager &manager = LogManager::GetInstance();
  if (request == nullptr || response == nullptr){
    return;
  }
  auto logBytes = request->log().c_str();
  LOG_INFO("message len: %lu", request->log().size());
  char * workingPointer = (char *)logBytes;
  auto record_type = GetRecordTypeFromBytes(workingPointer);
  switch (record_type) {
    case LOGRECORD_TYPE_TRANSACTION_BEGIN:
    {
      TransactionRecord record(record_type);
      GetTransactionRecord(record, workingPointer);
      LogManager::GetInstance().GetFrontendLogger()->StartTransactionRecovery(record.GetTransactionId());
      break;
    }
    case LOGRECORD_TYPE_TRANSACTION_COMMIT:
    {
      TransactionRecord record(record_type);
      GetTransactionRecord(record, workingPointer);
      LogManager::GetInstance().GetFrontendLogger()->CommitTransactionRecovery(record.GetTransactionId());
      break;
    }
    case LOGRECORD_TYPE_WAL_TUPLE_INSERT:
    case LOGRECORD_TYPE_WAL_TUPLE_DELETE:
    case LOGRECORD_TYPE_WAL_TUPLE_UPDATE:
    {
      auto record = new TupleRecord(record_type);
      auto logger = LogManager::GetInstance().GetFrontendLogger();
      GetTupleRecord(*record, workingPointer, logger->GetRecoveryPool());
      logger->AddTupleRecord(record->GetTransactionId(), record);
      break;
    }
    default:
      LOG_INFO("Got Type as TXN_INVALID");
      break;
  }
  response->set_status(networking::LoggingStatus::ERROR);
  LOG_INFO("In log record replay service");
}

}  // namespace networking
}  // namespace peloton
