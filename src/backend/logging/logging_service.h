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

#pragma once

#include "backend/networking/logging_service.pb.h"
#include "backend/networking/rpc_server.h"
#include "backend/networking/peloton_endpoint.h"

#include <iostream>

namespace peloton {
namespace logging {

class LoggingService : public networking::PelotonLoggingService {
public:

    // implements LoggingService ------------------------------------------

    virtual void LogRecordReplay(::google::protobuf::RpcController* controller,
            const networking::LogRecordReplayRequest* request,
	    networking::LogRecordReplayResponse* response,
            ::google::protobuf::Closure* done);

};

//void StartPelotonService();

}  // namespace networking
}  // namespace peloton
