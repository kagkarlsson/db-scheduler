package com.github.kagkarlsson.scheduler.compatibility;

import ch.qos.logback.classic.Level;
import com.github.kagkarlsson.scheduler.DbUtils;
import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlExtension;
import com.github.kagkarlsson.scheduler.helper.ChangeLogLevelsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.sql.DataSource;

public class PostgresqlCompatibilityTest extends CompatibilityTest {

    @RegisterExtension
    public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();
//    Enable if test gets flaky!
//    @RegisterExtension
//    public ChangeLogLevelsExtension changeLogLevels = new ChangeLogLevelsExtension(
//        new ChangeLogLevelsExtension.LogLevelOverride("com.github.kagkarlsson.scheduler.DueExecutionsBatch", Level.TRACE),
//        new ChangeLogLevelsExtension.LogLevelOverride("com.github.kagkarlsson.scheduler.Waiter", Level.DEBUG),
//        new ChangeLogLevelsExtension.LogLevelOverride("com.github.kagkarlsson.scheduler.Scheduler", Level.DEBUG)
//    );


    @Override
    public DataSource getDataSource() {
        return postgres.getDataSource();
    }

    @Override
    public boolean commitWhenAutocommitDisabled() {
        return false;
    }

}
