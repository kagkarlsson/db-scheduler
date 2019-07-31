package com.github.kagkarlsson.examples.boot;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.boot.actuator.DbSchedulerHealthIndicator;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.assertj.AssertableWebApplicationContext;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.context.ConfigurableWebApplicationContext;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SmokeTest {
    @Autowired
    ConfigurableWebApplicationContext applicationContext;
    AssertableWebApplicationContext ctx;

    @Autowired
    DbSchedulerHealthIndicator healthIndicator;

    @Before
    public void setup() {
        this.ctx = AssertableWebApplicationContext.get(() -> applicationContext);
    }

    @Test
    public void it_should_load_context() {
        assertThat(ctx).hasNotFailed();
    }

    @Test
    public void it_should_have_a_scheduler_bean() {
        assertThat(ctx).hasSingleBean(Scheduler.class);
    }

    @Test
    public void it_should_have_two_tasks_exposed_as_beans() {
        assertThat(ctx.getBeansOfType(Task.class).values()).hasSize(2);
        assertThat(ctx.getBeansOfType(OneTimeTask.class).values()).hasSize(1);
        assertThat(ctx.getBeansOfType(RecurringTask.class).values()).hasSize(1);
    }

    @Test
    public void it_should_be_healthy_after_startup() {
        assertThat(healthIndicator.health().getStatus()).isEqualTo(Status.UP);
    }
}
