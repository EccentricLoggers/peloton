//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// configuration.cpp
//
// Identification: benchmark/ycsb/configuration.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

// #undef NDEBUG

#include <iomanip>
#include <algorithm>

#include "backend/benchmark/ycsb/ycsb_configuration.h"
#include "backend/common/logger.h"

#undef NDEBUG

namespace peloton {
namespace benchmark {
namespace ycsb {

void Usage(FILE *out) {
  fprintf(out,
          "Command line options : ycsb <options> \n"
          "   -h --help              :  Print help message \n"
          "   -k --scale_factor      :  # of tuples \n"
          "   -d --duration          :  execution duration \n"
          "   -s --snapshot_duration :  snapshot duration \n"
          "   -c --column_count      :  # of columns \n"
          "   -u --write_ratio       :  Fraction of updates \n"
          "   -b --backend_count     :  # of backends \n"
          "   -t --zipf_theta        :  theta to control skewness \n"
          "   -m --mix_txn           :  run read/write mix txn \n"
          "   -l --num_loggers       :  num_loggers ( >= 0 ) \n"
          "   -x --sync_commit       :  enable synchronous commit (0 or 1) \n"
          "   -q --flush_freq        :  set the frequency of log fsync\n");
  // TODO add description for wait_time, file_size, log_buffer_size,
  // checkpointer
  exit(EXIT_FAILURE);
}

static struct option opts[] = {
    {"scale_factor", optional_argument, NULL, 'k'},
    {"duration", optional_argument, NULL, 'd'},
    {"snapshot_duration", optional_argument, NULL, 's'},
    {"column_count", optional_argument, NULL, 'c'},
    {"update_ratio", optional_argument, NULL, 'u'},
    {"backend_count", optional_argument, NULL, 'b'},
    {"zipf_theta", optional_argument, NULL, 't'},
    {"mix_txn", no_argument, NULL, 'm'},
    {"enable_logging", optional_argument, NULL, 'l'},
    {"sync_commit", optional_argument, NULL, 'x'},
    {"wait_time", optional_argument, NULL, 'w'},
    {"file_size", optional_argument, NULL, 'f'},
    {"log_buffer_size", optional_argument, NULL, 'z'},
    {"checkpointer", optional_argument, NULL, 'p'},
    {"flush_freq", optional_argument, NULL, 'q'},
    {NULL, 0, NULL, 0}};

void ValidateScaleFactor(const configuration &state) {
  if (state.scale_factor <= 0) {
    LOG_ERROR("Invalid scale_factor :: %d", state.scale_factor);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %d", "scale_factor", state.scale_factor);
}

void ValidateColumnCount(const configuration &state) {
  if (state.column_count <= 0) {
    LOG_ERROR("Invalid column_count :: %d", state.column_count);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %d", "column_count", state.column_count);
}

void ValidateUpdateRatio(const configuration &state) {
  if (state.update_ratio < 0 || state.update_ratio > 1) {
    LOG_ERROR("Invalid update_ratio :: %lf", state.update_ratio);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %lf", "update_ratio", state.update_ratio);
}

void ValidateBackendCount(const configuration &state) {
  if (state.backend_count <= 0) {
    LOG_ERROR("Invalid backend_count :: %d", state.backend_count);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %d", "backend_count", state.backend_count);
}

void ValidateDuration(const configuration &state) {
  if (state.duration <= 0) {
    LOG_ERROR("Invalid duration :: %lf", state.duration);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %lf", "execution duration", state.duration);
}

void ValidateSnapshotDuration(const configuration &state) {
  if (state.snapshot_duration <= 0) {
    LOG_ERROR("Invalid snapshot_duration :: %lf", state.snapshot_duration);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %lf", "snapshot_duration", state.snapshot_duration);
}

void ValidateZipfTheta(const configuration &state) {
  if (state.zipf_theta < 0 || state.zipf_theta > 1.0) {
    LOG_ERROR("Invalid zipf_theta :: %lf", state.zipf_theta);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %lf", "zipf_theta", state.zipf_theta);
}

void ValidateLogging(const configuration &state) {
  // I tried setting NDEBUG but I still get an unused error
  (void) state;
  // TODO validate that sync_commit is enabled only when logging is enabled
  LOG_INFO("%s : %d", "num_loggers", state.num_loggers);
  LOG_INFO("%s : %d", "synchronous_commit", state.sync_commit);
  LOG_INFO("%s : %d", "wait_time", (int)state.wait_timeout);
}

// void ValidateFlushFreq(const configuration &state) {
//   if (state.flush_freq <= 0) {
//     LOG_ERROR("Invalid flush_freq :: %d", state.flush_freq);
//     exit(EXIT_FAILURE);
//   }

//   LOG_INFO("%s : %d microseconds", "flush_freq", state.flush_freq);
// }

void ParseArguments(int argc, char *argv[], configuration &state) {
  // Default Values
  state.scale_factor = 1;
  state.duration = 10;
  state.snapshot_duration = 0.1;
  state.column_count = 10;
  state.update_ratio = 0.5;
  state.backend_count = 2;
  state.zipf_theta = 0.0;
  state.run_mix = false;

  state.num_loggers = 0;
  state.sync_commit = 0;
  state.wait_timeout = 0;
  state.file_size = 32768;
  state.log_buffer_size = 32768;
  state.checkpointer = 0;
  state.flush_freq = 0;

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "ahmk:d:s:c:u:b:l:x:w:f:z:p:q:", opts, &idx);

    if (c == -1) break;

    switch (c) {
      case 'k':
        state.scale_factor = atoi(optarg);
        break;
      case 'd':
        state.duration = atof(optarg);
        break;
      case 's':
        state.snapshot_duration = atof(optarg);
        break;
      case 'c':
        state.column_count = atoi(optarg);
        break;
      case 'u':
        state.update_ratio = atof(optarg);
        break;
      case 'b':
        state.backend_count = atoi(optarg);
        break;
      case 't':
        state.zipf_theta = atof(optarg);
        break;
      case 'x':
        state.sync_commit = atoi(optarg);
        break;
      case 'l':
        state.num_loggers = atoi(optarg);
        break;
      case 'w':
        state.wait_timeout = atol(optarg);
        break;
      case 'f':
        state.file_size = atoi(optarg);
        break;
      case 'z':
        state.log_buffer_size = atoi(optarg);
        break;
      case 'p':
        state.checkpointer = atoi(optarg);
        break;
      case 'q':
        state.flush_freq = atoi(optarg);
        break;
      case 'm':
        state.run_mix = true;
        break;
      case 'h':
        Usage(stderr);
        exit(EXIT_FAILURE);
        break;
      default:
        fprintf(stderr, "\nUnknown option: -%c-\n", c);
        Usage(stderr);
        exit(EXIT_FAILURE);
    }
  }

  // Print configuration
  ValidateScaleFactor(state);
  ValidateColumnCount(state);
  ValidateUpdateRatio(state);
  ValidateBackendCount(state);
  ValidateLogging(state);
  ValidateDuration(state);
  ValidateSnapshotDuration(state);
  ValidateZipfTheta(state);
  //ValidateFlushFreq(state);
}

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
