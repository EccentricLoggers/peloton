#Project Specification for Multi-threaded Queries
## General information
Our project is about multi-threaded queries.
The way we do this is like this:

1.  Insert arbitrary exchange executors into a plan tree
2.  Launch multiple threads (actually use a thread pool) to do the work parallelly in each exchange executor
3.  Combine results from multiple sub-tasks
4.  Maintain the same interface (GetOutput) as other executors so that exchange executors can be combined with normal executors

## What we implemented

1.  Insert exchange executors into an executor tree.
2.  Exchange Sequential Scan Executor
3.  Exchange Hash Executor
4.  Exchange Hash Join Executor

## Relevant files
**Modified**:
```
  backend/bridge/dml/executor/plan_executor.cpp
  src/backend/executor/Makefile.am
  src/backend/executor/executors.h
  tests/executor/Makefile.am
```
**Created**:
```
  src/backend/bridge/dml/mapper/mapper_parallel_plan.cpp
  src/backend/common/barrier.h
  src/backend/planner/exchange_seq_scan_plan.h
  src/backend/planner/exchange_hash_plan.h
  src/backend/planner/exchange_hash_join_plan.h
  src/backend/executor/exchange_hash_executor.h
  src/backend/executor/exchange_hash_executor.cpp
  src/backend/executor/exchange_seq_scan_executor.h
  src/backend/executor/exchange_seq_scan_executor.cpp
  src/backend/executor/exchange_hash_join_executor.cpp
  src/backend/executor/exchange_hash_join_executor.h
  tests/executor/exchange_seq_scan_test.cpp
  tests/executor/exchange_hash_test.cpp
  tests/executor/exchange_hash_join_test.cpp
```

 **Of those, the following are essential**:
```
  backend/bridge/dml/executor/plan_executor.cpp
  src/backend/common/barrier.h
  src/backend/executor/exchange_hash_executor.cpp
  src/backend/executor/exchange_seq_scan_executor.h
  src/backend/executor/exchange_seq_scan_executor.cpp
  src/backend/executor/exchange_hash_join_executor.cpp
  src/backend/executor/exchange_hash_join_executor.h
  tests/executor/exchange_seq_scan_test.cpp
  tests/executor/exchange_hash_test.cpp
  tests/executor/exchange_hash_join_test.cpp
```


## How do we insert exchange executors?
Peloton derives plan trees from Postgres.
Postgres generates a plan tree corresponding to a query, Peloton then translate that tree into Peloton plan tree. At execution time, an executor tree is generated according to this plan tree and gets executed. Therefore, we have two places to insert exchange executors: either create our own version of exchange plan nodes (and then generate exchange executors based on exchange plan nodes) or directly generate exchange executors based on normal plan nodes.

We first tried the first approach. Then we think the second is better. The reasons are: 1) Our exchange plan nodes do not actually have anything different than the corresponding normal plan nodes; 2) Sharing same type of plan nodes is better for caching plan trees. Therefore, we choose the second approach in the end.

The code is at "src/backend/bridge/dml/executor/plan_executor.cpp" file "BuildExecutorTree" function.

The code for the first approach is at "src/backend/bridge/dml/mapper/mapper_parallel_plan.cpp", in case someone wants to switch to the first approach later (for example because exchange plan nodes need something different than normal plan nodes).

## How do exchange executors work?
It depends on the specific type of exchange executors. Basically, each exchange executor will divide input into multiple parts, wrap each part into a task, submit tasks to thread pool, and finally wait for all tasks to finish.
The implementation detail of specific exchange executors can be found at source code and its comment.

## Special attention
* The exchange sequential scan does not call transaction functions to ensure isolation. The reason can be found in source code comment.
*  Some tests (in exchange_hash_join_test.cpp and exchange_hash_test.cpp )are disabled b.c. large data tables should be built in the tests, which can be extremely slow when valgrind is on.
* The ExchangeHashJoinExectuor relies on boost::lockfree\_queue, which has some minor uinitialized value bug, thus the exchange\_hash\_join\_test cannot pass valgrind's check, and is currently commented (To add it back, just add it into the Makefile). However, ExchangeHashJoinExectuor can work and pass all the built in test and largeCorrectnessTest in exchange_hash_join_test.

* The `sleep_for()` function in ExchangeHashJoinExecutor is there because valgrind somehow causse the main thread to do busy loop, and other threads will not be scheduled. And `sleep_for()` will make the threads be scheduled as usual. It's notable that this function will rarely be called in common use. Because in common case, when other threads are working, there must be something to picked up in the queue.
