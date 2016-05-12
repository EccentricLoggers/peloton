#include <chrono>
#include <ctime>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "harness.h"

#include "backend/catalog/schema.h"
#include "backend/common/types.h"
#include "backend/common/value.h"
#include "backend/common/value_factory.h"
#include "backend/concurrency/transaction.h"
#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/executor/abstract_executor.h"
#include "backend/executor/exchange_seq_scan_executor.h"
#include "backend/executor/executor_context.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/executor/seq_scan_executor.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/expression_util.h"
#include "backend/planner/seq_scan_plan.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile_group_factory.h"

#include "executor/executor_tests_util.h"
#include "executor/mock_executor.h"
#include "harness.h"

using ::testing::NotNull;
using ::testing::Return;

namespace peloton {
namespace test {

class ExchangeSeqScanTests : public PelotonTest {};
// Set of tuples that gets selected in the predicate
const std::set<oid_t> g_tuple_ids({0, 3});

storage::DataTable *CreateTable(size_t tile_group_num, size_t row_num) {
  return ExecutorTestsUtil::CreateTable(tile_group_num, row_num);
}

expression::AbstractExpression *CreatePredicate(
    const std::set<oid_t> &tuple_ids) {
  assert(tuple_ids.size() >= 1);

  expression::AbstractExpression *predicate =
      expression::ExpressionUtil::ConstantValueFactory(Value::GetFalse());

  bool even = false;
  for (oid_t tuple_id : tuple_ids) {
    even = !even;

    // Create equality expression comparison tuple value and constant value.
    // First, create tuple value expression.
    expression::AbstractExpression *tuple_value_expr = nullptr;

    tuple_value_expr =
        even ? expression::ExpressionUtil::TupleValueFactory(0, 0)
             : expression::ExpressionUtil::TupleValueFactory(0, 3);

    // Second, create constant value expression.
    Value constant_value =
        even ? ValueFactory::GetIntegerValue(
                   ExecutorTestsUtil::PopulatedValue(tuple_id, 0))
             : ValueFactory::GetStringValue(std::to_string(
                   ExecutorTestsUtil::PopulatedValue(tuple_id, 3)));

    expression::AbstractExpression *constant_value_expr =
        expression::ExpressionUtil::ConstantValueFactory(constant_value);

    // Finally, link them together using an equality expression.
    expression::AbstractExpression *equality_expr =
        expression::ExpressionUtil::ComparisonFactory(
            EXPRESSION_TYPE_COMPARE_EQUAL, tuple_value_expr,
            constant_value_expr);

    // Join equality expression to other equality expression using ORs.
    predicate = expression::ExpressionUtil::ConjunctionFactory(
        EXPRESSION_TYPE_CONJUNCTION_OR, predicate, equality_expr);
  }

  return predicate;
}

/*
 * Run the given executor to fetch all results
 * Return the time it takes in milliseconds
 */
double RunTest(executor::AbstractExecutor &executor,
               std::vector<std::unique_ptr<executor::LogicalTile>> *result) {
  const auto start = std::chrono::system_clock::now();
  EXPECT_TRUE(executor.Init());
  while (executor.Execute()) {
    if (result) {
      result->emplace_back(executor.GetOutput());
    }
  }
  const auto end = std::chrono::system_clock::now();
  const std::chrono::duration<double> diff = end - start;
  const double ms = diff.count() * 1000;
  return ms;
}

TEST_F(ExchangeSeqScanTests, LeafNodeCorrectnessTest) {
  constexpr size_t tile_num = 10;
  constexpr size_t row_num = 100;
  // Create table.
  std::unique_ptr<storage::DataTable> table(CreateTable(tile_num, row_num));

  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids({0, 1, 3});

  // Create plan node.
  planner::SeqScanPlan node(table.get(), CreatePredicate(g_tuple_ids),
                            column_ids);

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  executor::ExchangeSeqScanExecutor executor(&node, context.get());
  std::vector<std::unique_ptr<executor::LogicalTile>> result;
  RunTest(executor, &result);

  size_t expected_num_tiles = table->GetTileGroupCount();
  size_t expected_num_cols = column_ids.size();

  EXPECT_EQ(tile_num, expected_num_tiles);
  EXPECT_EQ(expected_num_tiles, result.size());

  // Check correctness of result tiles.
  for (size_t i = 0; i < expected_num_tiles; i++) {
    EXPECT_EQ(expected_num_cols, result[i]->GetColumnCount());

    // Only tuples per tile satisfy our predicate.
    EXPECT_EQ(g_tuple_ids.size(), result[i]->GetTupleCount());

    // Verify values.
    std::set<oid_t> expected_tuples_left(g_tuple_ids);
    for (oid_t new_tuple_id : *(result[i])) {
      // We divide by 10 because we know how PopulatedValue() computes.
      // Bad style. Being a bit lazy here...

      int old_tuple_id =
          result[i]->GetValue(new_tuple_id, 0).GetIntegerForTestsOnly() / 10;

      EXPECT_EQ(1, expected_tuples_left.erase(old_tuple_id));

      int val1 = ExecutorTestsUtil::PopulatedValue(old_tuple_id, 1);
      EXPECT_EQ(val1,
                result[i]->GetValue(new_tuple_id, 1).GetIntegerForTestsOnly());
      int val2 = ExecutorTestsUtil::PopulatedValue(old_tuple_id, 3);

      // expected_num_cols - 1 is a hacky way to ensure that
      // we are always getting the last column in the original table.
      // For the tile group test case, it'll be 2 (one column is removed
      // during the scan as part of the test case).
      // For the logical tile test case, it'll be 3.
      Value string_value(ValueFactory::GetStringValue(std::to_string(val2)));
      EXPECT_EQ(string_value,
                result[i]->GetValue(new_tuple_id, expected_num_cols - 1));
    }
    EXPECT_EQ(0, expected_tuples_left.size());
  }

  txn_manager.CommitTransaction();
}

/*
TEST_F(ExchangeSeqScanTests, LeafNodeSpeedTest) {
  constexpr size_t tile_num = 100000;
  constexpr size_t row_num = 1000;
  // Create table.
  std::unique_ptr<storage::DataTable> table(CreateTable(tile_num, row_num));

  LOG_INFO("CreateTable done");

  // Parallel version
  double time = 0;
  for (int i = 0; i < 10; ++i) {
    LOG_INFO("iteration %d", i + 1);
    // Column ids to be added to logical tile after scan.
    std::vector<oid_t> column_ids({0, 1, 3});

    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

    // Parallel version
    planner::SeqScanPlan node(table.get(), CreatePredicate(g_tuple_ids),
                              column_ids);
    auto txn = txn_manager.BeginTransaction();
    std::unique_ptr<executor::ExecutorContext> context(
        new executor::ExecutorContext(txn));
    executor::ExchangeSeqScanExecutor executor(&node, context.get());
    std::vector<std::unique_ptr<executor::LogicalTile>> result;
    double duration1 = RunTest(executor, &result);
    time += duration1;
    txn_manager.CommitTransaction();
    LOG_INFO("parallel: %lf ms", duration1);
  }
  LOG_INFO("parallel average time: %lf ms", time / 10);

  // Sequential version
  {
    std::vector<oid_t> column_ids({0, 1, 3});
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

    // Single thread version
    planner::SeqScanPlan node2(table.get(), CreatePredicate(g_tuple_ids),
                               column_ids);
    auto txn2 = txn_manager.BeginTransaction();
    std::unique_ptr<executor::ExecutorContext> context2(
        new executor::ExecutorContext(txn2));
    executor::SeqScanExecutor executor2(&node2, context2.get());
    std::vector<std::unique_ptr<executor::LogicalTile>> result2;
    double duration2 = RunTest(executor2, &result2);
    txn_manager.CommitTransaction();
    LOG_INFO("single thread: %lf ms", duration2);
  }
}
*/
}
}