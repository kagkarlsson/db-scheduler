# Benchmark

See the [Performance](../README.md#performance) section of the README for an explanation of the
`fetch` and `lock-and-fetch` polling strategies referenced below.

## Benchmark test

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

Currently, polling strategy `lock-and-fetch` is implemented only for Postgres (single statement mode), SQL Server and MySQL v8+ (generic mode). Contributions adding support for more databases are welcome.

## User testimonial

There are a number of users that are using db-scheduler for high throughput use-cases. See for example:

* https://github.com/kagkarlsson/db-scheduler/issues/209#issuecomment-1026699872
* https://github.com/kagkarlsson/db-scheduler/issues/190#issuecomment-805867950
