package com.github.kagkarlsson.scheduler.logging;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class LogHelperTest {
    private Logger logger = (Logger) LoggerFactory.getLogger(this.getClass());
    private ListAppender<ILoggingEvent> appender = new ListAppender<>();

    {
        logger.setLevel(ch.qos.logback.classic.Level.ALL);
        appender.start();
    }

    @BeforeEach
    public void addAppender() {
        logger.addAppender(appender);
    }

    @AfterEach
    public void removeAppender() {
        logger.detachAppender(appender);
    }

    @ParameterizedTest
    @EnumSource(Level.class)
    public void should_log_using_extracted_log_method(Level level) {
        LogHelper.LogMethod logMethod = LogHelper.getLogMethod(logger, level);
        logMethod.log("test {}", "test");

        ILoggingEvent logEvent = appender.list.get(0);
        assertThat(logEvent.getLevel().levelStr, is(level.name()));
        assertThat(logEvent.getFormattedMessage(), is("test test"));
    }

}
