#include <chrono>
#include <memory>

#include "harness.h"

#include "backend/common/types.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/logical_tile_factory.h"

#include "backend/executor/exchange_hash_executor.h"
#include "backend/executor/hash_executor.h"
#include "backend/executor/hash_join_executor.h"
#include "backend/executor/merge_join_executor.h"
#include "backend/executor/nested_loop_join_executor.h"

#include "backend/expression/abstract_expression.h"
#include "backend/expression/expression_util.h"
#include "backend/expression/tuple_value_expression.h"

#include "backend/planner/hash_join_plan.h"
#include "backend/planner/hash_plan.h"
#include "backend/planner/merge_join_plan.h"
#include "backend/planner/nested_loop_join_plan.h"

#include "backend/storage/data_table.h"
#include "backend/storage/tile.h"
#include "backend/storage/tile_group_factory.h"

#include "backend/concurrency/transaction_manager_factory.h"

#include "executor/executor_tests_util.h"
#include "executor/join_tests_util.h"
#include "mock_executor.h"

using ::testing::NotNull;
using ::testing::Return;
using ::testing::InSequence;

namespace peloton {
namespace test {

class ExchangeHashExecutorTests : public PelotonTest {};

storage::DataTable *CreateTable(size_t tile_group_num, size_t row_num) {
  return ExecutorTestsUtil::CreateTable(tile_group_num, row_num);
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

/*
 * Build two hash tables on the same table using HashExecutor and
 * ExchangeHashExecutor
 * Compare the results (which should be identical)
 * A little confusion clarification:
 * There is only one table, even though its name (riglt_table) somehow indicates
 * there might be a left one.
 * The reason we use right table as variable name in the test is because
 * normally, hash executor is the right child of hash join executor and it
 * consumes the right
 * table of a hash join executor.
 */
TEST_F(ExchangeHashExecutorTests, CorrectnessTest) {
  constexpr size_t tile_num = 30;
  constexpr size_t row_num = 10;

  constexpr size_t right_table_tile_group_count = tile_num;

  // Create table.
  std::unique_ptr<storage::DataTable> right_table(CreateTable(tile_num, row_num));

  LOG_INFO("CreateTable done");

  MockExecutor right_table_scan_executor;

  // Create tile groups
  std::vector<std::unique_ptr<executor::LogicalTile>>
      right_table_logical_tile_ptrs;
  for (size_t right_table_tile_group_itr = 0;
       right_table_tile_group_itr < right_table_tile_group_count;
       right_table_tile_group_itr++) {
    std::unique_ptr<executor::LogicalTile> right_table_logical_tile(
        executor::LogicalTileFactory::WrapTileGroup(
            right_table->GetTileGroup(right_table_tile_group_itr)));
    right_table_logical_tile_ptrs.push_back(
        std::move(right_table_logical_tile));
  }

  // Right scan executor returns logical tiles from the right table

  EXPECT_CALL(right_table_scan_executor, DInit()).WillOnce(Return(true));

  ExpectNormalTileResults(right_table_tile_group_count,
                          &right_table_scan_executor,
                          right_table_logical_tile_ptrs);

  // Create hash keys
  expression::AbstractExpression *right_table_attr_1 =
      new expression::TupleValueExpression(1, 1);

  std::vector<std::unique_ptr<const expression::AbstractExpression>> hash_keys;
  hash_keys.emplace_back(right_table_attr_1);

  // Create hash plan node
  planner::HashPlan hash_plan_node(hash_keys);

  // Construct the hash executor
  executor::HashExecutor hash_executor(&hash_plan_node, nullptr);
  hash_executor.AddChild(&right_table_scan_executor);

  EXPECT_TRUE(hash_executor.Init());
  EXPECT_TRUE(hash_executor.Execute());

  // Create a same copy for the other hash executor
  MockExecutor right_table_scan_executor2;

  std::vector<std::unique_ptr<executor::LogicalTile>>
      right_table_logical_tile_ptrs2;

  for (size_t right_table_tile_group_itr = 0;
       right_table_tile_group_itr < right_table_tile_group_count;
       right_table_tile_group_itr++) {
    std::unique_ptr<executor::LogicalTile> right_table_logical_tile(
        executor::LogicalTileFactory::WrapTileGroup(
            right_table->GetTileGroup(right_table_tile_group_itr)));
    right_table_logical_tile_ptrs2.push_back(
        std::move(right_table_logical_tile));
  }

  // Right scan executor returns logical tiles from the right table

  EXPECT_CALL(right_table_scan_executor2, DInit()).WillOnce(Return(true));

  ExpectNormalTileResults(right_table_tile_group_count,
                          &right_table_scan_executor2,
                          right_table_logical_tile_ptrs2);

  // Create hash keys
  expression::AbstractExpression *right_table_attr_12 =
      new expression::TupleValueExpression(1, 1);

  std::vector<std::unique_ptr<const expression::AbstractExpression>> hash_keys2;
  hash_keys2.emplace_back(right_table_attr_12);

  // Create hash plan node
  planner::HashPlan hash_plan_node2(hash_keys2);

  // Construct the hash executor
  executor::ExchangeHashExecutor parallel_hash_executor(&hash_plan_node2,
                                                        nullptr);
  parallel_hash_executor.AddChild(&right_table_scan_executor2);

  EXPECT_TRUE(parallel_hash_executor.Init());
  EXPECT_TRUE(parallel_hash_executor.Execute());

  // Compare the result hash table of the two hash executors
  // Loop through table 1, ensure that every entry in table 1 is in table 2.
  // Delete the corresponding entry in table 2 for every entry seen in table 1
  // Ensure in the end we have an empty table 2
  auto &hash_table = hash_executor.GetHashTable();
  auto &hash_table2 = parallel_hash_executor.GetHashTable();
  LOG_INFO("hash table size=%lu, concurrent hash table size=%lu",
           (unsigned long)hash_table.size(), (unsigned long)hash_table2.size());
  {
    for (const auto &iter1 : hash_table) {
      EXPECT_TRUE(hash_table2.contains(iter1.first));
      executor::ExchangeHashExecutor::MapValueType set;
      bool found = hash_table2.find(iter1.first, set);
      EXPECT_TRUE(found);
      for (const auto &iter12 : iter1.second) {
        auto iter22 = set.find(iter12);
        EXPECT_TRUE(iter22 != set.end());
        set.erase(iter22);
      }
      EXPECT_TRUE(set.empty());
      bool erased = hash_table2.erase(iter1.first);
      EXPECT_TRUE(erased);
    }
    EXPECT_TRUE(hash_table2.empty());
  }
}

/*
TEST_F(ExchangeHashExecutorTests, SpeedTest) {
  constexpr size_t tile_num = 300;
  constexpr size_t row_num = 100000;

  // Create table.
  std::unique_ptr<storage::DataTable> right_table(CreateTable(tile_num, row_num));

  LOG_INFO("CreateTable done");

  // Parallel version
  double time = 0;
  for(int i=0; i<10; ++i) {
    LOG_INFO("iteration %d", i+1);
    MockExecutor right_table_scan_executor;

    std::vector<std::unique_ptr<executor::LogicalTile>>
            right_table_logical_tile_ptrs;
    constexpr size_t right_table_tile_group_count = tile_num;

    for(size_t right_table_tile_group_itr = 0;
        right_table_tile_group_itr<right_table_tile_group_count;
        right_table_tile_group_itr++) {
      std::unique_ptr<executor::LogicalTile> right_table_logical_tile(
              executor::LogicalTileFactory::WrapTileGroup(
                      right_table->GetTileGroup(right_table_tile_group_itr)));
      right_table_logical_tile_ptrs.push_back(
              std::move(right_table_logical_tile));
    }

    // Right scan executor returns logical tiles from the right table

    EXPECT_CALL(right_table_scan_executor, DInit()).WillOnce(Return(true));

    ExpectNormalTileResults(
            right_table_tile_group_count,
            &right_table_scan_executor,
            right_table_logical_tile_ptrs);

    // Create hash keys
    expression::AbstractExpression *right_table_attr_1 =
            new expression::TupleValueExpression(1, 1);

    std::vector<std::unique_ptr<const expression::AbstractExpression>>
            hash_keys;
    hash_keys.emplace_back(right_table_attr_1);

    // Create hash plan node
    planner::HashPlan hash_plan_node(hash_keys);

    // Construct the hash executor
    executor::ExchangeHashExecutor hash_executor(&hash_plan_node, nullptr);
    hash_executor.AddChild(&right_table_scan_executor);

    const auto start = std::chrono::system_clock::now();
    EXPECT_TRUE(hash_executor.Init());
    EXPECT_TRUE(hash_executor.Execute());
    const auto end = std::chrono::system_clock::now();
    const std::chrono::duration<double> diff = end-start;
    const double ms = diff.count()*1000;
    time += ms;
    LOG_INFO("ExchangeHashExecutor execution time: %lf ms", ms);
  }
  LOG_INFO("ExchangeHashExecutor average time: %lf ms", time/10);

  // Sequential version
  {
    MockExecutor right_table_scan_executor;

    std::vector<std::unique_ptr<executor::LogicalTile>>
            right_table_logical_tile_ptrs;
    constexpr size_t right_table_tile_group_count = tile_num;

    for(size_t right_table_tile_group_itr = 0;
        right_table_tile_group_itr<right_table_tile_group_count;
        right_table_tile_group_itr++) {
      std::unique_ptr<executor::LogicalTile> right_table_logical_tile(
              executor::LogicalTileFactory::WrapTileGroup(
                      right_table->GetTileGroup(right_table_tile_group_itr)));
      right_table_logical_tile_ptrs.push_back(
              std::move(right_table_logical_tile));
    }

    // Right scan executor returns logical tiles from the right table

    EXPECT_CALL(right_table_scan_executor, DInit()).WillOnce(Return(true));

    ExpectNormalTileResults(
            right_table_tile_group_count,
            &right_table_scan_executor,
            right_table_logical_tile_ptrs);

    // Create hash keys
    expression::AbstractExpression *right_table_attr_1 =
            new expression::TupleValueExpression(1, 1);

    std::vector<std::unique_ptr<const expression::AbstractExpression>>
            hash_keys;
    hash_keys.emplace_back(right_table_attr_1);

    // Create hash plan node
    planner::HashPlan hash_plan_node(hash_keys);

    // Construct the hash executor
    executor::HashExecutor hash_executor(&hash_plan_node, nullptr);
    hash_executor.AddChild(&right_table_scan_executor);

    const auto start = std::chrono::system_clock::now();
    EXPECT_TRUE(hash_executor.Init());
    EXPECT_TRUE(hash_executor.Execute());
    const auto end = std::chrono::system_clock::now();
    const std::chrono::duration<double> diff = end-start;
    const double ms = diff.count()*1000;
    LOG_INFO("HashExecutor execution time: %lf ms", ms);
  }
}
*/
}
}
