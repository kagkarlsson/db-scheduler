# Performance

While db-scheduler initially was targeted at low-to-medium throughput use-cases, it handles high-throughput use-cases (1000+ executions/second) quite well
due to the fact that its data-model is very simple, consisting of a single table of executions.
To understand how it will perform, it is useful to consider the SQL statements it runs per batch of executions.

## Polling strategy fetch

The original and default polling strategy, `fetch`, will do the following:
1. `select` a batch of due executions
2. For every execution, on execute, try to `update` the execution to `picked=true` for this scheduler-instance. May miss due to competing schedulers.
3. If execution was picked, when execution is done, `update` or `delete` the record according to handlers.

In sum per batch: 1 select, 2 * batch-size updates   (excluding misses)

## Polling strategy lock-and-fetch

In v10, a new polling strategy (`lock-and-fetch`) was added. It utilizes the fact that most databases now have support for `SKIP LOCKED` in `SELECT FOR UPDATE` statements (see [2ndquadrant blog](https://www.2ndquadrant.com/en/blog/what-is-select-skip-locked-for-in-postgresql-9-5/)).
Using such a strategy, it is possible to fetch executions pre-locked, and thus getting one statement less:

1. `select for update .. skip locked` a batch of due executions. These will already be picked by the scheduler-instance.
2. When execution is done, `update` or `delete` the record according to handlers.

In sum per batch: 1 select-and-update, 1 * batch-size updates (no misses)

`lock-and-fetch` is currently implemented only for Postgres (single-statement mode), SQL Server and
MySQL v8+ (generic mode); see [Database compatibility](../README.md#database-compatibility).

## Benchmark

To get an idea of what to expect from db-scheduler, see results from the tests run in GCP below.
Tests were run with a few different configurations, but each using 4 competing scheduler-instances running on separate VMs.
TPS is the approx. transactions per second as shown in GCP.

|                                        | Throughput fetch (ex/s) | TPS fetch (estimates) | Throughput lock-and-fetch (ex/s) | TPS lock-and-fetch (estimates) |
|----------------------------------------|-------------------------|-----------------------|----------------------------------|--------------------------------|
| Postgres 4core 25gb ram, 4xVMs(2-core) |                         |                       |                                  |                                |
| 20 threads, lower 4.0, upper 20.0      | 2000                    | 9000                  | 10600                            | 11500                          |
| 100 threads, lower 2.0, upper 6.0      | 2560                    | 11000                 | 11200                            | 11200                          |
|                                        |                         |                       |                                  |                                |
| Postgres 8core 50gb ram, 4xVMs(4-core) |                         |                       |                                  |                                |
| 50 threads, lower: 0.5, upper: 4.0     | 4000                    | 22000                 | 11840                            | 10300                          |
|                                        |                         |                       |                                  |                                |

Observations for these tests:

* For `fetch`
  * TPS ≈ 4-5 * execution-throughput. A bit higher than the best-case 2 * execution-throughput, likely due the inefficiency of missed executions.
  * throughput did scale with postgres instance-size, from 2000 executions/s on 4core to 4000 executions/s on 8core
* For `lock-and-fetch`
  * TPS ≈ 1 * execution-throughput. As expected.
  * seem to consistently handle 10k executions/s for these configurations
  * throughput did not scale with postgres instance-size (4-8 core), so bottleneck is somewhere else

## User testimonial

There are a number of users that are using db-scheduler for high throughput use-cases. See for example:

* https://github.com/kagkarlsson/db-scheduler/issues/209#issuecomment-1026699872
* https://github.com/kagkarlsson/db-scheduler/issues/190#issuecomment-805867950

