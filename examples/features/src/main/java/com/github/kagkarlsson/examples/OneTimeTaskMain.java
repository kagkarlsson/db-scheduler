package com.github.kagkarlsson.examples;

import com.github.kagkarlsson.examples.helpers.Example;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;

import javax.sql.DataSource;
import java.io.Serializable;
import java.time.Instant;

public class OneTimeTaskMain extends Example {

    public static void main(String[] args) {
        new OneTimeTaskMain().runWithDatasource();
    }

    @Override
    public void run(DataSource dataSource) {

        OneTimeTask<MyTaskData> myAdhocTask = Tasks.oneTime("my-typed-adhoc-task", MyTaskData.class)
            .execute((inst, ctx) -> {
                System.out.println("Executed! Custom data, Id: " + inst.getData().id);
            });

        final Scheduler scheduler = Scheduler
            .create(dataSource, myAdhocTask)
            .threads(5)
            .build();

        scheduler.start();

        // Schedule the task for execution a certain time in the future and optionally provide custom data for the execution
        scheduler.schedule(myAdhocTask.instance("1045", new MyTaskData(1001L)), Instant.now().plusSeconds(5));
    }

    public static class MyTaskData implements Serializable {
        public final long id;

        public MyTaskData(long id) {
            this.id = id;
        }
    }

}
