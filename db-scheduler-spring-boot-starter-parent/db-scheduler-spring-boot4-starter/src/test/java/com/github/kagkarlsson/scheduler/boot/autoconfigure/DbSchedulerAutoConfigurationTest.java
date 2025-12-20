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
import com.github.kagkarlsson.scheduler.boot.config.startup.ContextReadyStart;
import com.github.kagkarlsson.scheduler.boot.config.startup.ImmediateStart;
import com.github.kagkarlsson.scheduler.boot.testconfig.CustomStarterConfiguration;
import com.github.kagkarlsson.scheduler.boot.testconfig.CustomStatsRegistryConfiguration;
import com.github.kagkarlsson.scheduler.boot.testconfig.MultipleTasksConfiguration;
import com.github.kagkarlsson.scheduler.boot.testconfig.SingleTaskConfiguration;
import com.github.kagkarlsson.scheduler.stats.MicrometerStatsRegistry;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry.DefaultStatsRegistry;
import com.github.kagkarlsson.scheduler.task.Task;
import java.util.Map;
import javax.sql.DataSource;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.LazyInitializationExcludeFilter;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.health.autoconfigure.contributor.HealthContributorAutoConfiguration;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;
import org.springframework.boot.micrometer.metrics.autoconfigure.CompositeMeterRegistryAutoConfiguration;
import org.springframework.boot.micrometer.metrics.autoconfigure.MetricsAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.jdbc.Sql.ExecutionPhase;

/**
 * SpringBootTest version with isolated scenarios using @Nested classes. SQL schema is loaded via
 * application-integration-test.properties.
 */
@ActiveProfiles("integration-test")
@Sql(
    scripts = "classpath:schema/init_schema.sql",
    executionPhase = ExecutionPhase.BEFORE_TEST_CLASS)
@Sql(
    scripts = "classpath:schema/truncate_schema.sql",
    executionPhase = ExecutionPhase.BEFORE_TEST_METHOD)
class DbSchedulerAutoConfigurationTest {

  @ImportAutoConfiguration({
    DataSourceAutoConfiguration.class,
    MetricsAutoConfiguration.class,
    CompositeMeterRegistryAutoConfiguration.class,
    DbSchedulerMetricsAutoConfiguration.class,
    DbSchedulerActuatorAutoConfiguration.class,
    DbSchedulerAutoConfiguration.class
  })
  static class CommonAutoConfig {}

  @Nested
  @SpringBootTest(classes = {CommonAutoConfig.class})
  @TestPropertySource(properties = {"db-scheduler.delay-startup-until-context-ready=true"})
  class DefaultsAndHealthAndPriority {

    @Autowired ApplicationContext ctx;

    @Test
    void it_should_initialize_an_empty_scheduler() {
      assertSingleBean(DataSource.class);
      assertSingleBean(Scheduler.class);

      ctx.getBean(Scheduler.class)
          .fetchScheduledExecutions(
              execution -> fail("No scheduled executions should be present", execution));
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
  @SpringBootTest(classes = {CommonAutoConfig.class})
  @ImportAutoConfiguration(
      exclude = {
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
  @SpringBootTest(classes = {CommonAutoConfig.class})
  @TestPropertySource(properties = {"db-scheduler.enabled=false"})
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
  @SpringBootTest(classes = {CommonAutoConfig.class})
  @TestPropertySource(properties = {"db-scheduler.delay-startup-until-context-ready=false"})
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
  @SpringBootTest(classes = {CommonAutoConfig.class})
  @TestPropertySource(properties = {"db-scheduler.delay-startup-until-context-ready=true"})
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
   * ------------------------------------------------------------------------- */
  @Nested
  @SpringBootTest(classes = {CommonAutoConfig.class, SingleTaskConfiguration.class})
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
  @SpringBootTest(classes = {CommonAutoConfig.class, MultipleTasksConfiguration.class})
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
  @SpringBootTest(
      classes = {
        CommonAutoConfig.class,
        SingleTaskConfiguration.class,
        CustomStarterConfiguration.class
      })
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
  @SpringBootTest(classes = {CommonAutoConfig.class})
  class WithMicrometer {

    @Autowired ApplicationContext ctx;

    @Test
    void it_should_provide_micrometer_registry_if_micrometer_is_present() {
      assertThat(ctx.getBeansOfType(MicrometerStatsRegistry.class)).hasSize(1);
    }
  }

  @Nested
  @SpringBootTest(
      classes = {SingleTaskConfiguration.class}) // do NOT load CommonAutoConfig to remove metrics
  @ImportAutoConfiguration({
    DataSourceAutoConfiguration.class,
    HealthContributorAutoConfiguration.class,
    DbSchedulerMetricsAutoConfiguration.class,
    DbSchedulerActuatorAutoConfiguration.class,
    DbSchedulerAutoConfiguration.class
    // MetricsAutoConfiguration and CompositeMeterRegistryAutoConfiguration NOT imported
  })
  class WithoutMicrometerRegistryBean {

    @Autowired ApplicationContext ctx;

    @Test
    void it_should_provide_noop_registry_if_micrometer_not_present() {
      assertThat(ctx.getBeansOfType(DefaultStatsRegistry.class)).hasSize(1);
      assertThat(ctx.getBeansOfType(MicrometerStatsRegistry.class)).isEmpty();
    }
  }

  @Nested
  @SpringBootTest(classes = {SingleTaskConfiguration.class})
  @ImportAutoConfiguration({
    DataSourceAutoConfiguration.class,
    // Actuator/metrics excluded
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
  @SpringBootTest(
      classes = {
        CommonAutoConfig.class,
        SingleTaskConfiguration.class,
        CustomStatsRegistryConfiguration.class
      })
  class WithCustomStatsRegistry {

    @Autowired ApplicationContext ctx;

    @Test
    void it_should_use_custom_stats_registry_if_present_in_context() {
      assertThat(ctx.getBeansOfType(StatsRegistry.class)).hasSize(1);
      assertThat(ctx.getBean("customStatsRegistry"));
    }
  }
}
