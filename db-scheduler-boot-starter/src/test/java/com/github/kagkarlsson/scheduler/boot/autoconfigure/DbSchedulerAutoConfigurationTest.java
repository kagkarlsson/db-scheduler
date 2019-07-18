package com.github.kagkarlsson.scheduler.boot.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.github.kagkarlsson.scheduler.Scheduler;
import javax.sql.DataSource;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

@RunWith(JUnit4.class)
public class DbSchedulerAutoConfigurationTest {
    private final ApplicationContextRunner ctxRunner;

    public DbSchedulerAutoConfigurationTest() {
        ctxRunner = new ApplicationContextRunner()
            .withPropertyValues(
                "spring.application.name=db-scheduler-boot-starter-test",
                "spring.profiles.active=integration-test"
            ).withConfiguration(AutoConfigurations.of(
                DataSourceAutoConfiguration.class, DbSchedulerAutoConfiguration.class
            ));
    }

    @Test
    public void it_should_initialize_an_empty_scheduler() {
        ctxRunner.run((AssertableApplicationContext ctx) -> {
            assertThat(ctx).hasSingleBean(DataSource.class);
            assertThat(ctx).hasSingleBean(Scheduler.class);

            ctx.getBean(Scheduler.class).getScheduledExecutions(execution -> {
                fail("No scheduled executions should be present", execution);
            });
        });
    }
}
