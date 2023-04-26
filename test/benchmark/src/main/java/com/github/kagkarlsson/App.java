/**
 * Copyright (C) Gustav Karlsson
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.zaxxer.hikari.HikariDataSource;

import java.util.concurrent.TimeUnit;

public class App {
    public static void main(String[] args) {
        MetricRegistry metrics = new MetricRegistry();

        ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build();
        reporter.start(1, TimeUnit.SECONDS);

        HikariDataSource ds = new HikariDataSource();
        String databaseHost = System.getenv("PGHOST");
        String password = System.getenv("PGPASSWORD");
        ds.setJdbcUrl("jdbc:postgresql://"+databaseHost+"/bench");
        ds.setUsername("gustavkarlsson");
        ds.setPassword(password);


        OneTimeTask<Void> task1 = Tasks.oneTime("task1").execute((taskInstance, executionContext) -> {
            //System.out.println("Ran task1, instance " + taskInstance.getId());
        });

        Scheduler scheduler = Scheduler.create(ds, task1)
            .pollUsingLockAndFetch(0.5, 4.0)
//            .pollUsingFetchAndLockOnExecute(6, 40.0)
//            .pollUsingFetchAndLockOnExecute(0.5, 4.0)
            .statsRegistry(new BenchmarkStatsRegistry(metrics))
            .threads(50)
            .build();
        Runtime.getRuntime().addShutdownHook(new Thread(scheduler::stop));

        scheduler.start();
    }

}
