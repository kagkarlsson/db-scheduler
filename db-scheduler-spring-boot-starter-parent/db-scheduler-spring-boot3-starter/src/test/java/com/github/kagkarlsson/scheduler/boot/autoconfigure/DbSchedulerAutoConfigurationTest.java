package com.github.kagkarlsson.scheduler.boot.autoconfigure;

import static com.github.kagkarlsson.scheduler.SchedulerBuilder.DEFAULT_DELETION_OF_UNRESOLVED_TASKS_DURATION;
import static com.github.kagkarlsson.scheduler.SchedulerBuilder.DEFAULT_HEARTBEAT_INTERVAL;
import static com.github.kagkarlsson.scheduler.SchedulerBuilder.DEFAULT_MISSED_HEARTBEATS_LIMIT;
import static com.github.kagkarlsson.scheduler.SchedulerBuilder.DEFAULT_POLLING_INTERVAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.boot.actuator.DbSchedulerHealthIndicator;
import com.github.kagkarlsson.scheduler.boot.config.DbSchedulerCustomizer;
import com.github.kagkarlsson.scheduler.boot.config.DbSchedulerProperties;
import com.github.kagkarlsson.scheduler.boot.config.DbSchedulerStarter;
import com.github.kagkarlsson.scheduler.boot.config.startup.AbstractSchedulerStarter;
import com.github.kagkarlsson.scheduler.boot.config.startup.ContextReadyStart;
import com.github.kagkarlsson.scheduler.boot.config.startup.ImmediateStart;
import com.github.kagkarlsson.scheduler.stats.MicrometerStatsRegistry;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry.DefaultStatsRegistry;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry.CandidateStatsEvent;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry.ExecutionStatsEvent;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry.SchedulerStatsEvent;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import java.util.Map;
import java.util.Objects;
import javax.sql.DataSource;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.LazyInitializationExcludeFilter;
import org.springframework.boot.actuate.autoconfigure.health.HealthContributorAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.metrics.CompositeMeterRegistryAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.metrics.MetricsAutoConfiguration;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.jdbc.Sql;

/**
 * SpringBootTest version with isolated scenarios using @Nested classes.
 * SQL schema is loaded via application-integration-test.properties.
 */
@ActiveProfiles("integration-test")
@Sql(scripts = "classpath:schema.sql")
class DbSchedulerAutoConfigurationTest {

  private static final Logger log =
    LoggerFactory.getLogger(DbSchedulerAutoConfigurationTest.class);

  /* -------------------------------------------------------------------------
   *  Common configuration: enable required auto-configurations.
   * ------------------------------------------------------------------------- */
  @ImportAutoConfiguration({
    DataSourceAutoConfiguration.class,
    MetricsAutoConfiguration.class,
    CompositeMeterRegistryAutoConfiguration.class,
    HealthContributorAutoConfiguration.class,
    DbSchedulerMetricsAutoConfiguration.class,
    DbSchedulerActuatorAutoConfiguration.class,
    DbSchedulerAutoConfiguration.class
  })
  static class CommonAutoConfig { }

  /* -------------------------------------------------------------------------
   *  Schema loading via spring.sql.init.* (schema.sql file on classpath)
   *  - If JPA is used elsewhere: add spring.jpa.defer-datasource-initialization=true
   * ------------------------------------------------------------------------- */
  @Nested
  @SpringBootTest(classes = { CommonAutoConfig.class })
  @TestPropertySource(properties = {
    "db-scheduler.delay-startup-until-context-ready=true"
  })
  class DefaultsAndHealthAndPriority {

    @Autowired ApplicationContext ctx;

    @Test
    @Sql(scripts = "classpath:schema.sql")
    void it_should_initialize_an_empty_scheduler() {
      assertSingleBean(DataSource.class);
      assertSingleBean(Scheduler.class);

      ctx.getBean(Scheduler.class)
        .fetchScheduledExecutions(execution ->
          fail("No scheduled executions should be present", execution));
    }

    @Test
    void it_should_use_the_default_values_from_library() {
      DbSchedulerProperties props = ctx.getBean(DbSchedulerProperties.class);
      assertThat(props.getPollingInterval()).isEqualTo(DEFAULT_POLLING_INTERVAL);
      assertThat(props.getHeartbeatInterval()).isEqualTo(DEFAULT_HEARTBEAT_INTERVAL);
      assertThat(props.getMissedHeartbeatsLimit()).isEqualTo(DEFAULT_MISSED_HEARTBEATS_LIMIT);
      assertThat(props.getDeleteUnresolvedAfter())
        .isEqualTo(DEFAULT_DELETION_OF_UNRESOLVED_TASKS_DURATION);
    }

    @Test
    void it_should_autoconfigure_a_health_check() {
      assertSingleBean(DbSchedulerHealthIndicator.class);
    }

    @Test
    void it_should_have_priority_disabled_by_default() {
      DbSchedulerProperties props = ctx.getBean(DbSchedulerProperties.class);
      assertThat(props.isPriorityEnabled()).isFalse();
    }

    @Test
    void it_should_exclude_db_scheduler_starter_from_lazy_init() {
      LazyInitializationExcludeFilter filter = ctx.getBean(LazyInitializationExcludeFilter.class);
      assertThat(filter.isExcluded(null, null, DbSchedulerStarter.class)).isTrue();
    }

    /* Util */
    private <T> void assertSingleBean(Class<T> type) {
      Map<String, T> beans = ctx.getBeansOfType(type);
      assertThat(beans).hasSize(1);
    }
  }

  /* -------------------------------------------------------------------------
   *  Actuator absent => no health indicator
   * ------------------------------------------------------------------------- */
  @Nested
  @SpringBootTest(classes = { CommonAutoConfig.class })
  @ImportAutoConfiguration(exclude = {
    HealthContributorAutoConfiguration.class,
    DbSchedulerActuatorAutoConfiguration.class
  })
  class WithoutActuatorHealth {

    @Autowired ApplicationContext ctx;

    @Test
    void it_should_not_autoconfigure_a_health_check_when_actuator_is_absent() {
      assertThat(ctx.getBeansOfType(DbSchedulerHealthIndicator.class)).isEmpty();
    }
  }

  /* -------------------------------------------------------------------------
   *  db-scheduler.enabled=false => skip all autoconfig
   * ------------------------------------------------------------------------- */
  @Nested
  @SpringBootTest(classes = { CommonAutoConfig.class })
  @TestPropertySource(properties = {
    "db-scheduler.enabled=false"
  })
  class DisabledAutoConfiguration {

    @Autowired ApplicationContext ctx;

    @Test
    void it_should_skip_autoconfiguration_if_explicitly_disabled() {
      assertThat(ctx.getBeansOfType(Scheduler.class)).isEmpty();
      assertThat(ctx.getBeansOfType(DbSchedulerStarter.class)).isEmpty();
      assertThat(ctx.getBeansOfType(DbSchedulerCustomizer.class)).isEmpty();
      assertThat(ctx.getBeansOfType(DbSchedulerHealthIndicator.class)).isEmpty();
      assertThat(ctx.getBeansOfType(StatsRegistry.class)).isEmpty();
    }
  }

  /* -------------------------------------------------------------------------
   *  Startup strategies
   * ------------------------------------------------------------------------- */
  @Nested
  @SpringBootTest(classes = { CommonAutoConfig.class })
  @TestPropertySource(properties = {
    "db-scheduler.delay-startup-until-context-ready=false"
  })
  class StartAsSoonAsPossible {

    @Autowired ApplicationContext ctx;

    @Test
    void it_should_start_as_soon_as_possible() {
      assertThat(ctx.getBeansOfType(Scheduler.class)).hasSize(1);
      assertThat(ctx.getBeansOfType(DbSchedulerStarter.class)).hasSize(1);
      assertThat(ctx.getBeansOfType(ImmediateStart.class)).hasSize(1);
      assertThat(ctx.getBeansOfType(ContextReadyStart.class)).isEmpty();
    }
  }

  @Nested
  @SpringBootTest(classes = { CommonAutoConfig.class })
  @TestPropertySource(properties = {
    "db-scheduler.delay-startup-until-context-ready=true"
  })
  class StartWhenContextReady {

    @Autowired ApplicationContext ctx;

    @Test
    void it_should_start_when_the_context_is_ready() {
      assertThat(ctx.getBeansOfType(Scheduler.class)).hasSize(1);
      assertThat(ctx.getBeansOfType(DbSchedulerStarter.class)).hasSize(1);
      assertThat(ctx.getBeansOfType(ContextReadyStart.class)).hasSize(1);
      assertThat(ctx.getBeansOfType(ImmediateStart.class)).isEmpty();
    }
  }

  /* -------------------------------------------------------------------------
   *  Tasks (single / multiple)
   *  -> avoid inheritance that duplicated the "singleStringTask" bean
   * ------------------------------------------------------------------------- */
  @Nested
  @SpringBootTest(classes = { CommonAutoConfig.class, SingleTaskConfiguration.class })
  class WithSingleTask {

    @Autowired ApplicationContext ctx;

    @Test
    void it_should_initialize_a_scheduler_with_a_single_task() {
      assertThat(ctx.getBeansOfType(Scheduler.class)).hasSize(1);
      assertThat(ctx.getBeansOfType(Task.class)).hasSize(1);
      assertThat(ctx.getBean("singleStringTask")).isInstanceOf(Task.class);
    }
  }

  @Nested
  @SpringBootTest(classes = { CommonAutoConfig.class, MultipleTasksConfiguration.class })
  class WithMultipleTasks {

    @Autowired ApplicationContext ctx;

    @Test
    void it_should_initialize_a_scheduler_with_a_multiple_tasks() {
      assertThat(ctx.getBeansOfType(Scheduler.class)).hasSize(1);
      assertThat(ctx.getBean("firstTask")).isInstanceOf(Task.class);
      assertThat(ctx.getBean("secondTask")).isInstanceOf(Task.class);
      assertThat(ctx.getBean("thirdTask")).isInstanceOf(Task.class);
    }
  }

  /* -------------------------------------------------------------------------
   *  Custom startup strategy (via custom DbSchedulerStarter)
   * ------------------------------------------------------------------------- */
  @Nested
  @SpringBootTest(classes = { CommonAutoConfig.class, SingleTaskConfiguration.class, CustomStarterConfiguration.class })
  class WithCustomStarter {

    @Autowired ApplicationContext ctx;

    @Test
    void it_should_support_custom_starting_strategies() {
      assertThat(ctx.getBeansOfType(Scheduler.class)).hasSize(1);
      assertThat(ctx.getBeansOfType(DbSchedulerStarter.class)).hasSize(1);
      assertThat(ctx.getBeansOfType(ContextReadyStart.class)).isEmpty();
      assertThat(ctx.getBeansOfType(ImmediateStart.class)).isEmpty();
    }
  }

  /* -------------------------------------------------------------------------
   *  Micrometer present => MicrometerStatsRegistry
   *  Micrometer absent (no MeterRegistry bean) => DefaultStatsRegistry
   * ------------------------------------------------------------------------- */
  @Nested
  @SpringBootTest(classes = { CommonAutoConfig.class, SingleTaskConfiguration.class })
  class WithMicrometer {

    @Autowired ApplicationContext ctx;

    @Test
    void it_should_provide_micrometer_registry_if_micrometer_is_present() {
      assertThat(ctx.getBeansOfType(MicrometerStatsRegistry.class)).hasSize(1);
    }
  }

  @Nested
  @SpringBootTest(classes = { SingleTaskConfiguration.class }) // do NOT load CommonAutoConfig to remove metrics
  @ImportAutoConfiguration({
    DataSourceAutoConfiguration.class,
    HealthContributorAutoConfiguration.class,
    DbSchedulerMetricsAutoConfiguration.class,
    DbSchedulerActuatorAutoConfiguration.class,
    DbSchedulerAutoConfiguration.class
    // ⚠️ MetricsAutoConfiguration and CompositeMeterRegistryAutoConfiguration NOT imported
  })
  class WithoutMicrometerRegistryBean {

    @Autowired ApplicationContext ctx;

    @Test
    void it_should_provide_noop_registry_if_micrometer_not_present() {
      // No MeterRegistry in context -> lib must create DefaultStatsRegistry
      assertThat(ctx.getBeansOfType(DefaultStatsRegistry.class)).hasSize(1);
      assertThat(ctx.getBeansOfType(MicrometerStatsRegistry.class)).isEmpty();
    }
  }

  @Nested
  @SpringBootTest(classes = { SingleTaskConfiguration.class })
  @ImportAutoConfiguration({
    DataSourceAutoConfiguration.class,
    // Actuator/metrics excluded to simulate the other scenario from your original class
    DbSchedulerMetricsAutoConfiguration.class,
    DbSchedulerActuatorAutoConfiguration.class,
    DbSchedulerAutoConfiguration.class
  })
  class WithoutActuatorButExpectNoop { // analogous to your "no metrics auto-config" runner

    @Autowired ApplicationContext ctx;

    @Test
    void it_should_provide_noop_registry_if_actuator_not_present() {
      assertThat(ctx.getBeansOfType(DefaultStatsRegistry.class)).hasSize(1);
    }
  }

  /* -------------------------------------------------------------------------
   *  Custom StatsRegistry provided by the user
   * ------------------------------------------------------------------------- */
  @Nested
  @SpringBootTest(classes = { CommonAutoConfig.class, SingleTaskConfiguration.class, CustomStatsRegistry.class })
  class WithCustomStatsRegistry {

    @Autowired ApplicationContext ctx;

    @Test
    void it_should_use_custom_stats_registry_if_present_in_context() {
      assertThat(ctx.getBeansOfType(StatsRegistry.class)).hasSize(1);
      assertThat(ctx.getBeansOfType(DefaultStatsRegistry.class)).isEmpty();
      assertThat(ctx.getBeansOfType(MicrometerStatsRegistry.class)).isEmpty();
    }
  }

  /* =======================================================================
   *  Test support configurations (WITHOUT inheritance between them)
   * ======================================================================= */
  @Configuration(proxyBeanMethods = false)
  static class SingleTaskConfiguration {
    @Bean("singleStringTask")
    Task<String> singleStringTask() {
      return namedStringTask("single-string-task");
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class MultipleTasksConfiguration {
    @Bean Task<String> firstTask()  { return namedStringTask("first-task"); }
    @Bean Task<String> secondTask() { return namedStringTask("second-task"); }
    @Bean Task<String> thirdTask()  { return namedStringTask("third-task"); }
  }

  @Configuration(proxyBeanMethods = false)
  static class CustomStarterConfiguration {
    @Bean
    DbSchedulerStarter someCustomStarter(Scheduler scheduler) {
      return new SomeCustomStarter(scheduler);
    }
    static class SomeCustomStarter extends AbstractSchedulerStarter {
      SomeCustomStarter(Scheduler scheduler) {
        super(scheduler);
        try {
          log.info("Thinking 5 seconds before starting the scheduler");
          Thread.sleep(5_000);
          doStart();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class CustomStatsRegistry {
    @Bean
    StatsRegistry customStatsRegistry() {
      return new StatsRegistry() {
        public void register(SchedulerStatsEvent e) {}
        public void register(CandidateStatsEvent e) {}
        public void register(ExecutionStatsEvent e) {}
        public void registerSingleCompletedExecution(ExecutionComplete e) {}
      };
    }
  }

  private static Task<String> namedStringTask(String name) {
    Objects.requireNonNull(name);
    return Tasks.oneTime(name, String.class)
      .execute((instance, context) ->
        log.info("Executed task: {}, ctx: {}", instance, context));
  }
}
