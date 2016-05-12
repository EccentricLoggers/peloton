#include <memory>

#include "harness.h"

#include "backend/common/types.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/logical_tile_factory.h"

#include "backend/executor/exchange_hash_executor.h"
#include "backend/executor/exchange_hash_join_executor.h"
#include "backend/executor/hash_executor.h"
#include "backend/executor/hash_join_executor.h"

#include "backend/expression/abstract_expression.h"
#include "backend/expression/expression_util.h"
#include "backend/expression/tuple_value_expression.h"

#include "backend/planner/hash_join_plan.h"
#include "backend/planner/hash_plan.h"

#include "backend/storage/data_table.h"
#include "backend/storage/tile.h"

#include "backend/concurrency/transaction_manager_factory.h"

#include "executor/executor_tests_util.h"
#include "executor/join_tests_util.h"
#include "mock_executor.h"

using ::testing::NotNull;
using ::testing::Return;
using ::testing::InSequence;

#define DISABLE_INDEX false

// uncomment this line to enable speed_test
//#define SPEEDTEST_ON

namespace peloton {
namespace test {


class ExchangeHashJoinTests : public PelotonTest {};

// Utility to build large test tables.
// Example:
//    BuildTestTableUtil join_test;
//    join_test.CreateTestTable(1000, 3000, 20, false);
class BuildTestTableUtil {
 public:
  // Execute join test according to given params
  // params set_workload and workload is used to set the workload
  // of a thread.
  void ExecuteJoinTest(PlanNodeType join_algorithm, PelotonJoinType join_type,
                       oid_t join_test_type, bool set_workload = false,
                       size_t workload = 150);

  // Create two testTables (left and right table) for exchange hash join test
  // with given group_size, left_group_num and right_group_num.
  // if param is_large_para_test is true, then will create special tables.
  // If this function has been called before, then it will directly return.
  void CreateTestTable(size_t group_size, size_t left_group_num,
                       size_t right_group_num, bool is_large_para_test) {
    if (table_created_) {
      return;
    }
    tile_group_size = group_size;
    left_table_tile_group_count = left_group_num;
    right_table_tile_group_count = right_group_num;

    LOG_INFO("CreateTestTable...\n");
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

    const auto start = std::chrono::system_clock::now();

    txn_manager.BeginTransaction();

    left_table_.reset(ExecutorTestsUtil::CreateTable(
          static_cast<int>(tile_group_size), DISABLE_INDEX));

    if (is_large_para_test) {
      ExecutorTestsUtil::PopulateTableForParallelTest(
          left_table_.get(), tile_group_size * left_table_tile_group_count,
          true);
    } else {
      ExecutorTestsUtil::PopulateTable(
          left_table_.get(), tile_group_size * left_table_tile_group_count,
          false, false, false);
    }

    right_table_.reset(ExecutorTestsUtil::CreateTable(
          static_cast<int>(tile_group_size), DISABLE_INDEX));

    if (is_large_para_test) {
      ExecutorTestsUtil::PopulateTableForParallelTest(
          right_table_.get(), tile_group_size * right_table_tile_group_count,
          false);
    } else {
      ExecutorTestsUtil::PopulateTable(
          right_table_.get(), tile_group_size * right_table_tile_group_count,
          false, false, false);
    }

    txn_manager.CommitTransaction();

    const auto end = std::chrono::system_clock::now();
    const std::chrono::duration<double> diff = end - start;
    const double ms = diff.count() * 1000;
    LOG_INFO(
        "CreateTest Table, tile_group_size:%lu, left:%lu, right:%lu, takes %lf "
        "ms\n",
        tile_group_size, left_table_tile_group_count,
        right_table_tile_group_count, ms);
    table_created_ = true;
  }

  static size_t tile_group_size;
  static size_t left_table_tile_group_count;
  static size_t right_table_tile_group_count;

 private:
  std::unique_ptr<storage::DataTable> left_table_;
  std::unique_ptr<storage::DataTable> right_table_;
  bool table_created_ = false;
};

// initialization of default values to build test tables
size_t BuildTestTableUtil::tile_group_size = 5;
size_t BuildTestTableUtil::left_table_tile_group_count = 3;
size_t BuildTestTableUtil::right_table_tile_group_count = 2;

std::shared_ptr<const peloton::catalog::Schema> CreateJoinSchema() {
  return std::shared_ptr<const peloton::catalog::Schema>(new catalog::Schema(
      {ExecutorTestsUtil::GetColumnInfo(1), ExecutorTestsUtil::GetColumnInfo(1),
       ExecutorTestsUtil::GetColumnInfo(0),
       ExecutorTestsUtil::GetColumnInfo(0)}));
}

std::vector<PlanNodeType> join_algorithms = {PLAN_NODE_TYPE_EXCHANGE_HASH_JOIN};

std::vector<PelotonJoinType> join_types = {JOIN_TYPE_INNER, JOIN_TYPE_LEFT,
                                           JOIN_TYPE_RIGHT, JOIN_TYPE_OUTER};

oid_t CountTuplesWithNullFields(executor::LogicalTile *logical_tile);

void ValidateJoinLogicalTile(executor::LogicalTile *logical_tile,
                             bool if_print);

void ExpectEmptyTileResult(MockExecutor *table_scan_executor);

void ExpectMoreThanOneTileResults(
    MockExecutor *table_scan_executor,
    std::vector<std::unique_ptr<executor::LogicalTile>>
        &table_logical_tile_ptrs);

void ExpectNormalTileResults(size_t table_tile_group_count,
                             MockExecutor *table_scan_executor,
                             std::vector<std::unique_ptr<executor::LogicalTile>>
                                 &table_logical_tile_ptrs);

enum JOIN_TEST_TYPE {
  BASIC_TEST = 0,
  BOTH_TABLES_EMPTY = 1,
  COMPLICATED_TEST = 2,
  SPEED_TEST = 3,
  LEFT_TABLE_EMPTY = 4,
  RIGHT_TABLE_EMPTY = 5,
  LargeTableCorrectnessTest = 10,
};

void BuildTestTableUtil::ExecuteJoinTest(PlanNodeType join_algorithm,
                                         PelotonJoinType join_type,
                                         oid_t join_test_type,
                                         bool set_workload, size_t workload) {
  //===--------------------------------------------------------------------===//
  // Mock table scan executors
  //===--------------------------------------------------------------------===//

  MockExecutor left_table_scan_executor, right_table_scan_executor;

  if (join_test_type == COMPLICATED_TEST) {
    // Modify some values in left and right tables for complicated test
    auto left_source_tile = left_table_->GetTileGroup(2)->GetTile(0);
    auto right_dest_tile = right_table_->GetTileGroup(1)->GetTile(0);
    auto right_source_tile = left_table_->GetTileGroup(0)->GetTile(0);

    auto source_tile_tuple_count = left_source_tile->GetAllocatedTupleCount();
    auto source_tile_column_count = left_source_tile->GetColumnCount();

    // LEFT - 3 rd tile --> RIGHT - 2 nd tile
    for (oid_t tuple_itr = 3; tuple_itr < source_tile_tuple_count;
         tuple_itr++) {
      for (oid_t col_itr = 0; col_itr < source_tile_column_count; col_itr++) {
        right_dest_tile->SetValue(
            left_source_tile->GetValue(tuple_itr, col_itr), tuple_itr, col_itr);
      }
    }

    // RIGHT - 1 st tile --> RIGHT - 2 nd tile
    // RIGHT - 2 nd tile --> RIGHT - 2 nd tile
    for (oid_t col_itr = 0; col_itr < source_tile_column_count; col_itr++) {
      right_dest_tile->SetValue(right_source_tile->GetValue(4, col_itr), 0,
                                col_itr);
      right_dest_tile->SetValue(right_dest_tile->GetValue(3, col_itr), 2,
                                col_itr);
    }
  }

  std::vector<std::unique_ptr<executor::LogicalTile>>
      left_table_logical_tile_ptrs;
  std::vector<std::unique_ptr<executor::LogicalTile>>
      right_table_logical_tile_ptrs;

  // Wrap the input tables with logical tiles
  for (size_t left_table_tile_group_itr = 0;
       left_table_tile_group_itr <
       BuildTestTableUtil::left_table_tile_group_count;
       left_table_tile_group_itr++) {
    std::unique_ptr<executor::LogicalTile> left_table_logical_tile(
        executor::LogicalTileFactory::WrapTileGroup(
            left_table_->GetTileGroup(left_table_tile_group_itr)));
    left_table_logical_tile_ptrs.push_back(std::move(left_table_logical_tile));
  }

  for (size_t right_table_tile_group_itr = 0;
       right_table_tile_group_itr <
       BuildTestTableUtil::right_table_tile_group_count;
       right_table_tile_group_itr++) {
    std::unique_ptr<executor::LogicalTile> right_table_logical_tile(
        executor::LogicalTileFactory::WrapTileGroup(
            right_table_->GetTileGroup(right_table_tile_group_itr)));
    right_table_logical_tile_ptrs.push_back(
        std::move(right_table_logical_tile));
  }

  // Left scan executor returns logical tiles from the left table

  EXPECT_CALL(left_table_scan_executor, DInit()).WillOnce(Return(true));

  //===--------------------------------------------------------------------===//
  // Setup left table
  //===--------------------------------------------------------------------===//
  if (join_test_type == BASIC_TEST || join_test_type == COMPLICATED_TEST ||
      join_test_type == SPEED_TEST ||
      join_test_type == LargeTableCorrectnessTest) {
    ExpectNormalTileResults(BuildTestTableUtil::left_table_tile_group_count,
                            &left_table_scan_executor,
                            left_table_logical_tile_ptrs);

  } else if (join_test_type == BOTH_TABLES_EMPTY) {
    ExpectEmptyTileResult(&left_table_scan_executor);
  } else if (join_test_type == LEFT_TABLE_EMPTY) {
    ExpectEmptyTileResult(&left_table_scan_executor);
  } else if (join_test_type == RIGHT_TABLE_EMPTY) {
    // if (join_type == JOIN_TYPE_INNER || join_type == JOIN_TYPE_RIGHT) {
    if ((join_type == JOIN_TYPE_INNER || join_type == JOIN_TYPE_RIGHT) &&
        (join_algorithm != PLAN_NODE_TYPE_EXCHANGE_HASH_JOIN)) {
      ExpectMoreThanOneTileResults(&left_table_scan_executor,
                                   left_table_logical_tile_ptrs);
    } else {
      ExpectNormalTileResults(BuildTestTableUtil::left_table_tile_group_count,
                              &left_table_scan_executor,
                              left_table_logical_tile_ptrs);
    }
  }

  // Right scan executor returns logical tiles from the right table

  EXPECT_CALL(right_table_scan_executor, DInit()).WillOnce(Return(true));

  //===--------------------------------------------------------------------===//
  // Setup right table
  //===--------------------------------------------------------------------===//

  if (join_test_type == BASIC_TEST || join_test_type == COMPLICATED_TEST ||
      join_test_type == SPEED_TEST ||
      join_test_type == LargeTableCorrectnessTest) {
    ExpectNormalTileResults(BuildTestTableUtil::right_table_tile_group_count,
                            &right_table_scan_executor,
                            right_table_logical_tile_ptrs);

  } else if (join_test_type == BOTH_TABLES_EMPTY) {
    ExpectEmptyTileResult(&right_table_scan_executor);

  } else if (join_test_type == LEFT_TABLE_EMPTY) {
    if (join_type == JOIN_TYPE_INNER || join_type == JOIN_TYPE_LEFT) {
      // For hash join, we always build the hash table from right child
      if ((join_algorithm == PLAN_NODE_TYPE_HASHJOIN ||
           join_algorithm == PLAN_NODE_TYPE_EXCHANGE_HASH_JOIN)) {
        ExpectNormalTileResults(
            BuildTestTableUtil::right_table_tile_group_count,
            &right_table_scan_executor, right_table_logical_tile_ptrs);
      } else {
        ExpectMoreThanOneTileResults(&right_table_scan_executor,
                                     right_table_logical_tile_ptrs);
      }

    } else if ((join_type == JOIN_TYPE_OUTER || join_type == JOIN_TYPE_RIGHT)) {
      ExpectNormalTileResults(BuildTestTableUtil::right_table_tile_group_count,
                              &right_table_scan_executor,
                              right_table_logical_tile_ptrs);
    }
  } else if (join_test_type == RIGHT_TABLE_EMPTY) {
    ExpectEmptyTileResult(&right_table_scan_executor);
  }

  //===--------------------------------------------------------------------===//
  // Setup join plan nodes and executors and run them
  //===--------------------------------------------------------------------===//

  oid_t result_tuple_count = 0;
  oid_t tuples_with_null = 0;
  auto projection = JoinTestsUtil::CreateProjection();
  // setup the projection schema
  auto schema = CreateJoinSchema();

  // Construct predicate
  std::unique_ptr<const expression::AbstractExpression> predicate(
      JoinTestsUtil::CreateJoinPredicate());

  // Differ based on join algorithm
  switch (join_algorithm) {
    case PLAN_NODE_TYPE_HASHJOIN: {
      // Create hash plan node
      expression::AbstractExpression *right_table_attr_1 =
          new expression::TupleValueExpression(1, 1);

      std::vector<std::unique_ptr<const expression::AbstractExpression>>
          hash_keys;
      hash_keys.emplace_back(right_table_attr_1);

      // Create hash plan node
      planner::HashPlan hash_plan_node(hash_keys);

      // Construct the hash executor
      executor::HashExecutor hash_executor(&hash_plan_node, nullptr);
      //  executor::ExchangeHashExecutor hash_executor(&exchange_hash_plan_node,
      //  nullptr);

      // Create hash join plan node.
      planner::HashJoinPlan hash_join_plan_node(join_type, std::move(predicate),
                                                std::move(projection), schema);

      // Construct the hash join executor
      executor::HashJoinExecutor hash_join_executor(&hash_join_plan_node,
                                                    nullptr);

      // Construct the executor tree
      hash_join_executor.AddChild(&left_table_scan_executor);
      hash_join_executor.AddChild(&hash_executor);

      hash_executor.AddChild(&right_table_scan_executor);

      const auto start = std::chrono::system_clock::now();

      // Run the hash_join_executor
      EXPECT_TRUE(hash_join_executor.Init());
      while (hash_join_executor.Execute() == true) {
        std::unique_ptr<executor::LogicalTile> result_logical_tile(
            hash_join_executor.GetOutput());

        if (result_logical_tile != nullptr) {
          result_tuple_count += result_logical_tile->GetTupleCount();
          tuples_with_null +=
              CountTuplesWithNullFields(result_logical_tile.get());
          ValidateJoinLogicalTile(result_logical_tile.get(), false);
          LOG_TRACE("%s", result_logical_tile->GetInfo().c_str());
        }
      }

      const auto end = std::chrono::system_clock::now();
      const std::chrono::duration<double> diff = end - start;
      const double ms = diff.count() * 1000;
      LOG_INFO("HashJoin takes %lf ms\n", ms);

    } break;

    case PLAN_NODE_TYPE_EXCHANGE_HASH_JOIN: {
      // Create hash plan node
      expression::AbstractExpression *right_table_attr_1 =
          new expression::TupleValueExpression(1, 1);

      std::vector<std::unique_ptr<const expression::AbstractExpression>>
          hash_keys;
      hash_keys.emplace_back(right_table_attr_1);

      // Create hash plan node
      planner::HashPlan exchange_hash_plan_node(hash_keys);

      // Construct the hash executor
      executor::ExchangeHashExecutor parallel_hash_executor(
          &exchange_hash_plan_node, nullptr);
      // executor::HashExecutor hash_executor(&hash_plan_node, nullptr);

      // Create hash join plan node.
      planner::HashJoinPlan exchange_hash_join_plan_node(
          join_type, std::move(predicate), std::move(projection), schema);

      // Construct the hash join executor
      executor::ExchangeHashJoinExecutor exchange_hash_join_executor(
          &exchange_hash_join_plan_node, nullptr);

      // Construct the executor tree
      exchange_hash_join_executor.AddChild(&left_table_scan_executor);
      exchange_hash_join_executor.AddChild(&parallel_hash_executor);

      parallel_hash_executor.AddChild(&right_table_scan_executor);

      if (set_workload) {
        exchange_hash_join_executor.SetTaskNumPerThread(workload);
      }

      const auto start = std::chrono::system_clock::now();
      // Run the hash_join_executor
      EXPECT_TRUE(exchange_hash_join_executor.Init());
      while (exchange_hash_join_executor.Execute() == true) {
        std::unique_ptr<executor::LogicalTile> result_logical_tile(
            exchange_hash_join_executor.GetOutput());

        if (result_logical_tile != nullptr) {
          result_tuple_count += result_logical_tile->GetTupleCount();
          tuples_with_null +=
              CountTuplesWithNullFields(result_logical_tile.get());
          ValidateJoinLogicalTile(result_logical_tile.get(), true);
          LOG_TRACE("%s", result_logical_tile->GetInfo().c_str());
        }
      }

      const auto end = std::chrono::system_clock::now();
      const std::chrono::duration<double> diff = end - start;
      const double ms = diff.count() * 1000;
      LOG_INFO("ExchangeHashJoin takes %lf ms\n", ms);

    } break;
    default:
      throw Exception("Unsupported join algorithm : " +
                      std::to_string(join_algorithm));
      break;
  }

  //===--------------------------------------------------------------------===//
  // Execute test
  //===--------------------------------------------------------------------===//

  if (join_test_type == BASIC_TEST) {
    // Check output
    switch (join_type) {
      case JOIN_TYPE_INNER:
        EXPECT_EQ(result_tuple_count, 10);
        EXPECT_EQ(tuples_with_null, 0);
        break;

      case JOIN_TYPE_LEFT:
        EXPECT_EQ(result_tuple_count, 15);
        EXPECT_EQ(tuples_with_null, 5);
        break;

      case JOIN_TYPE_RIGHT:
        EXPECT_EQ(result_tuple_count, 10);
        EXPECT_EQ(tuples_with_null, 0);
        break;

      case JOIN_TYPE_OUTER:
        EXPECT_EQ(result_tuple_count, 15);
        EXPECT_EQ(tuples_with_null, 5);
        break;

      default:
        throw Exception("Unsupported join type : " + std::to_string(join_type));
        break;
    }
  } else if (join_test_type == LargeTableCorrectnessTest) {
    // Check output
    LOG_INFO("RESULT ------ real result_tuple_count:%u, tuples_with_null:%u\n",
           result_tuple_count, tuples_with_null);
    switch (join_type) {
      case JOIN_TYPE_INNER:
        EXPECT_EQ(result_tuple_count, 54998);
        EXPECT_EQ(tuples_with_null, 0);
        break;

      case JOIN_TYPE_LEFT:
        EXPECT_EQ(result_tuple_count, 91000);
        EXPECT_EQ(tuples_with_null, 36002);
        break;

      case JOIN_TYPE_RIGHT:
        EXPECT_EQ(result_tuple_count, 58997);
        EXPECT_EQ(tuples_with_null, 3999);
        break;

      case JOIN_TYPE_OUTER:
        EXPECT_EQ(result_tuple_count, 94999);
        EXPECT_EQ(tuples_with_null, 40001);
        break;

      default:
        throw Exception("Unsupported join type : " + std::to_string(join_type));
        break;
    }
  } else if (join_test_type == BOTH_TABLES_EMPTY) {
    // Check output
    switch (join_type) {
      case JOIN_TYPE_INNER:
        EXPECT_EQ(result_tuple_count, 0);
        EXPECT_EQ(tuples_with_null, 0);
        break;

      case JOIN_TYPE_LEFT:
        EXPECT_EQ(result_tuple_count, 0);
        EXPECT_EQ(tuples_with_null, 0);
        break;

      case JOIN_TYPE_RIGHT:
        EXPECT_EQ(result_tuple_count, 0);
        EXPECT_EQ(tuples_with_null, 0);
        break;

      case JOIN_TYPE_OUTER:
        EXPECT_EQ(result_tuple_count, 0);
        EXPECT_EQ(tuples_with_null, 0);
        break;

      default:
        throw Exception("Unsupported join type : " + std::to_string(join_type));
        break;
    }

  } else if (join_test_type == COMPLICATED_TEST) {
    // Check output
    switch (join_type) {
      case JOIN_TYPE_INNER:
        EXPECT_EQ(result_tuple_count, 10);
        EXPECT_EQ(tuples_with_null, 0);
        break;

      case JOIN_TYPE_LEFT:
        EXPECT_EQ(result_tuple_count, 17);
        EXPECT_EQ(tuples_with_null, 7);
        break;

      case JOIN_TYPE_RIGHT:
        EXPECT_EQ(result_tuple_count, 10);
        EXPECT_EQ(tuples_with_null, 0);
        break;

      case JOIN_TYPE_OUTER:
        EXPECT_EQ(result_tuple_count, 17);
        EXPECT_EQ(tuples_with_null, 7);
        break;

      default:
        throw Exception("Unsupported join type : " + std::to_string(join_type));
        break;
    }

  } else if (join_test_type == LEFT_TABLE_EMPTY) {
    // Check output
    switch (join_type) {
      case JOIN_TYPE_INNER:
        EXPECT_EQ(result_tuple_count, 0);
        EXPECT_EQ(tuples_with_null, 0);
        break;

      case JOIN_TYPE_LEFT:
        EXPECT_EQ(result_tuple_count, 0);
        EXPECT_EQ(tuples_with_null, 0);
        break;

      case JOIN_TYPE_RIGHT:
        EXPECT_EQ(result_tuple_count, 10);
        EXPECT_EQ(tuples_with_null, 10);
        break;

      case JOIN_TYPE_OUTER:
        EXPECT_EQ(result_tuple_count, 10);
        EXPECT_EQ(tuples_with_null, 10);
        break;

      default:
        throw Exception("Unsupported join type : " + std::to_string(join_type));
        break;
    }
  } else if (join_test_type == RIGHT_TABLE_EMPTY) {
    // Check output
    switch (join_type) {
      case JOIN_TYPE_INNER:
        EXPECT_EQ(result_tuple_count, 0);
        EXPECT_EQ(tuples_with_null, 0);
        break;

      case JOIN_TYPE_LEFT:
        EXPECT_EQ(result_tuple_count, 15);
        EXPECT_EQ(tuples_with_null, 15);
        break;

      case JOIN_TYPE_RIGHT:
        EXPECT_EQ(result_tuple_count, 0);
        EXPECT_EQ(tuples_with_null, 0);
        break;

      case JOIN_TYPE_OUTER:
        EXPECT_EQ(result_tuple_count, 15);
        EXPECT_EQ(tuples_with_null, 15);
        break;

      default:
        throw Exception("Unsupported join type : " + std::to_string(join_type));
        break;
    }
  }
}

oid_t CountTuplesWithNullFields(executor::LogicalTile *logical_tile) {
  assert(logical_tile);

  // Get column count
  auto column_count = logical_tile->GetColumnCount();
  oid_t tuples_with_null = 0;

  // Go over the tile
  for (auto logical_tile_itr : *logical_tile) {
    const expression::ContainerTuple<executor::LogicalTile> join_tuple(
        logical_tile, logical_tile_itr);

    // Go over all the fields and check for null values
    for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
      auto val = join_tuple.GetValue(col_itr);
      if (val.IsNull()) {
        tuples_with_null++;
        break;
      }
    }
  }

  return tuples_with_null;
}

void ValidateJoinLogicalTile(executor::LogicalTile *logical_tile, bool) {
  assert(logical_tile);

  // Get column count
  auto column_count = logical_tile->GetColumnCount();

  // Check # of columns
  EXPECT_EQ(column_count, 4);

  // Check the attribute values
  // Go over the tile
  for (auto logical_tile_itr : *logical_tile) {
    const expression::ContainerTuple<executor::LogicalTile> join_tuple(
        logical_tile, logical_tile_itr);

    // Check the join fields
    auto left_tuple_join_attribute_val = join_tuple.GetValue(0);
    auto right_tuple_join_attribute_val = join_tuple.GetValue(1);

    EXPECT_TRUE(
        (left_tuple_join_attribute_val.IsNull() == true) ||
        (right_tuple_join_attribute_val.IsNull() == true) ||
        (left_tuple_join_attribute_val == right_tuple_join_attribute_val));
  }
}
void ExpectEmptyTileResult(MockExecutor *table_scan_executor) {
  // Expect zero result tiles from the child
  EXPECT_CALL(*table_scan_executor, DExecute()).WillOnce(Return(false));
}

void ExpectMoreThanOneTileResults(
    MockExecutor *table_scan_executor,
    std::vector<std::unique_ptr<executor::LogicalTile>> &

        table_logical_tile_ptrs) {
  // Expect more than one result tiles from the child, but only get one of them
  EXPECT_CALL(*table_scan_executor, DExecute()).WillOnce(Return(true));
  EXPECT_CALL(*table_scan_executor, GetOutput())
      .WillOnce(Return(table_logical_tile_ptrs[0].release()));
}

void ExpectNormalTileResults(size_t table_tile_group_count,
                             MockExecutor *table_scan_executor,
                             std::vector<std::unique_ptr<executor::LogicalTile>>
                                 &table_logical_tile_ptrs) {
  // Return true for the first table_tile_group_count times
  // Then return false after that
  {
    testing::Sequence execute_sequence;
    for (size_t table_tile_group_itr = 0;
         table_tile_group_itr < table_tile_group_count + 1;
         table_tile_group_itr++) {
      // Return true for the first table_tile_group_count times
      if (table_tile_group_itr < table_tile_group_count) {
        EXPECT_CALL(*table_scan_executor, DExecute())
            .InSequence(execute_sequence)
            .WillOnce(Return(true));
      } else  // Return false after that
      {
        EXPECT_CALL(*table_scan_executor, DExecute())
            .InSequence(execute_sequence)
            .WillOnce(Return(false));
      }
    }
  }
  // Return the appropriate logical tiles for the first table_tile_group_count
  // times
  {
    testing::Sequence get_output_sequence;
    for (size_t table_tile_group_itr = 0;
         table_tile_group_itr < table_tile_group_count;
         table_tile_group_itr++) {
      EXPECT_CALL(*table_scan_executor, GetOutput())
          .InSequence(get_output_sequence)
          .WillOnce(
              Return(table_logical_tile_ptrs[table_tile_group_itr].release()));
    }
  }
}

//////////////////////////////////////////////////
//                                              //
//                    Tests                     //
//                                              //
//////////////////////////////////////////////////

TEST_F(ExchangeHashJoinTests, BasicTest) {
  // Go over all join algorithms
  BuildTestTableUtil join_test;
  join_test.CreateTestTable(5, 3, 2, false);

  for (auto join_algorithm : join_algorithms) {
    LOG_INFO("JOIN ALGORITHM :: %s",
             PlanNodeTypeToString(join_algorithm).c_str());

    join_test.ExecuteJoinTest(join_algorithm, JOIN_TYPE_OUTER, BASIC_TEST);
  }
}

TEST_F(ExchangeHashJoinTests, EmptyTablesTest) {
  // Go over all join algorithms
  BuildTestTableUtil join_test;
  join_test.CreateTestTable(5, 3, 2, false);
  for (auto join_algorithm : join_algorithms) {
    LOG_INFO("JOIN ALGORITHM :: %s",
             PlanNodeTypeToString(join_algorithm).c_str());
    join_test.ExecuteJoinTest(join_algorithm, JOIN_TYPE_INNER,
                              BOTH_TABLES_EMPTY);
  }
}

TEST_F(ExchangeHashJoinTests, JoinTypesTest) {
  BuildTestTableUtil join_test;
  join_test.CreateTestTable(5, 3, 2, false);
  // Go over all join algorithms
  for (auto join_algorithm : join_algorithms) {
    LOG_INFO("JOIN ALGORITHM :: %s",
             PlanNodeTypeToString(join_algorithm).c_str());
    // Go over all join types
    for (auto join_type : join_types) {
      LOG_INFO("JOIN TYPE :: %d", join_type);
      // Execute the join test
      join_test.ExecuteJoinTest(join_algorithm, join_type, BASIC_TEST);
    }
  }
}

TEST_F(ExchangeHashJoinTests, ComplicatedTest) {
  // Go over all join algorithms
  BuildTestTableUtil join_test;
  join_test.CreateTestTable(5, 3, 2, false);
  for (auto join_algorithm : join_algorithms) {
    LOG_INFO("JOIN ALGORITHM :: %s",
             PlanNodeTypeToString(join_algorithm).c_str());
    // Go over all join types
    for (auto join_type : join_types) {
      LOG_INFO("JOIN TYPE :: %d", join_type);
      // Execute the join test
      join_test.ExecuteJoinTest(join_algorithm, join_type, COMPLICATED_TEST);
    }
  }
}

TEST_F(ExchangeHashJoinTests, LeftTableEmptyTest) {
  BuildTestTableUtil join_test;
  join_test.CreateTestTable(5, 3, 2, false);
  // Go over all join algorithms
  for (auto join_algorithm : join_algorithms) {
    LOG_INFO("JOIN ALGORITHM :: %s",
             PlanNodeTypeToString(join_algorithm).c_str());

    // Go over all join types
    for (auto join_type : join_types) {
      LOG_INFO("JOIN TYPE :: %d", join_type);
      // Execute the join test
      join_test.ExecuteJoinTest(join_algorithm, join_type, LEFT_TABLE_EMPTY);
    }
  }
}

TEST_F(ExchangeHashJoinTests, RightTableEmptyTest) {
  BuildTestTableUtil join_test;
  join_test.CreateTestTable(5, 3, 2, false);
  // Go over all join algorithms
  for (auto join_algorithm : join_algorithms) {
    LOG_INFO("JOIN ALGORITHM :: %s",
             PlanNodeTypeToString(join_algorithm).c_str());
    // Go over all join types
    for (auto join_type : join_types) {
      LOG_INFO("JOIN TYPE :: %d", join_type);
      // Execute the join test
      join_test.ExecuteJoinTest(join_algorithm, join_type, RIGHT_TABLE_EMPTY);
    }
  }
}

TEST_F(ExchangeHashJoinTests, JoinPredicateTest) {
  oid_t join_test_types = 1;
  BuildTestTableUtil join_test;
  join_test.CreateTestTable(5, 3, 2, false);
  // Go over all join test types
  for (oid_t join_test_type = 0; join_test_type < join_test_types;
       join_test_type++) {

    // Go over all join algorithms
    for (auto join_algorithm : join_algorithms) {
      LOG_INFO("JOIN ALGORITHM :: %s",
               PlanNodeTypeToString(join_algorithm).c_str());
      // Go over all join types
      for (auto join_type : join_types) {
        LOG_INFO("JOIN TYPE :: %d", join_type);
        // Execute the join test
        join_test.ExecuteJoinTest(join_algorithm, join_type, join_test_type);
      }
    }
  }
}

#ifdef SPEEDTEST_ON
TEST_F(ExchangeHashJoinTests, SpeedTest) {
  BuildTestTableUtil join_test;
  join_test.CreateTestTable(1000, 3000, 20, false);
  LOF_INFOFO("=================PLAN_NODE_TYPE_HASH_JOIN\n");
  join_test.ExecuteJoinTest(PLAN_NODE_TYPE_HASHJOIN, JOIN_TYPE_INNER,
SPEED_TEST);
  LOF_INFO("=================PLAN_NODE_TYPE_EXCHANGE_HASH_JOIN, 50\n");
  join_test.ExecuteJoinTest(PLAN_NODE_TYPE_EXCHANGE_HASH_JOIN, JOIN_TYPE_INNER,
SPEED_TEST, true, 50);
  LOF_INFO("=================PLAN_NODE_TYPE_EXCHANGE_HASH_JOIN, 100\n");
  join_test.ExecuteJoinTest(PLAN_NODE_TYPE_EXCHANGE_HASH_JOIN, JOIN_TYPE_INNER,
SPEED_TEST, true, 100);
  LOF_INFO("=================PLAN_NODE_TYPE_EXCHANGE_HASH_JOIN, 150\n");
  join_test.ExecuteJoinTest(PLAN_NODE_TYPE_EXCHANGE_HASH_JOIN, JOIN_TYPE_INNER,
SPEED_TEST, true, 150);
  LOF_INFO("=================PLAN_NODE_TYPE_EXCHANGE_HASH_JOIN, 200\n");
  join_test.ExecuteJoinTest(PLAN_NODE_TYPE_EXCHANGE_HASH_JOIN, JOIN_TYPE_INNER,
SPEED_TEST, true, 200);
  LOF_INFO("=================PLAN_NODE_TYPE_EXCHANGE_HASH_JOIN, 250\n");
  join_test.ExecuteJoinTest(PLAN_NODE_TYPE_EXCHANGE_HASH_JOIN, JOIN_TYPE_INNER,
SPEED_TEST, true, 250);
}

#endif /* SPEEDTEST_ON */

}  // namespace test
}  // namespace peloton
