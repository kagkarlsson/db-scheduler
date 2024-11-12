package com.github.kagkarlsson.examples.boot;

import static java.time.Duration.ofDays;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.boot.actuator.DbSchedulerHealthIndicator;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.assertj.AssertableWebApplicationContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.context.ConfigurableWebApplicationContext;

@ExtendWith(SpringExtension.class)
@SpringBootTest
public class SmokeTest {
  private static final Logger LOG = LoggerFactory.getLogger(SmokeTest.class);

  AssertableWebApplicationContext ctx;
  @Autowired ConfigurableWebApplicationContext applicationContext;
  @Autowired DbSchedulerHealthIndicator healthIndicator;
  @Autowired Task<Void> sampleOneTimeTask;
  @Autowired Scheduler scheduler;
  @Autowired TransactionTemplate tt;

  @BeforeEach
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
    assertThat(ctx.getBeansOfType(Task.class).values()).hasSizeGreaterThan(10);
  }

  @Test
  public void it_should_be_healthy_after_startup() {
    assertThat(healthIndicator.health().getStatus()).isEqualTo(Status.UP);
  }

  @Test
  public void it_should_manage_transactions_for_scheduler_client() {
    final TaskInstance<Void> instance1 = sampleOneTimeTask.instance("1");
    final TaskInstance<Void> instance2 = sampleOneTimeTask.instance("2");
    scheduler.schedule(instance1, Instant.now().plus(ofDays(1)));
    assertTrue(scheduler.getScheduledExecution(instance1).isPresent());

    try {
      tt.execute(
          status -> {
            scheduler.schedule(instance2, Instant.now().plus(ofDays(1)));
            throw new SimulatedException();
          });
    } catch (SimulatedException e) {
      LOG.info(
          "Caught expected SimulatedException. Should rollback ongoing transaction for instance2");
    }

    assertFalse(
        scheduler.getScheduledExecution(instance2).isPresent(),
        "instance2 should have been rolled back");
  }

  static class SimulatedException extends RuntimeException {}
}
