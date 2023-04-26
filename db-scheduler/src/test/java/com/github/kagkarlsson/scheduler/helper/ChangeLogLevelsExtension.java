package com.github.kagkarlsson.scheduler.helper;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.LoggerFactory;

public class ChangeLogLevelsExtension implements BeforeEachCallback, AfterEachCallback {

  private final LogLevelOverride[] overrides;

  public ChangeLogLevelsExtension(LogLevelOverride... overrides) {
    this.overrides = overrides;
  }

  @Override
  public void beforeEach(ExtensionContext extensionContext) throws Exception {
    Stream.of(overrides).forEach(LogLevelOverride::apply);
  }

  @Override
  public void afterEach(ExtensionContext extensionContext) {
    Stream.of(overrides).forEach(LogLevelOverride::reset);
  }

  public static class LogLevelOverride {
    private final String logger;
    private final Level levelOverride;
    private Level originalLevel;

    public LogLevelOverride(String logger, Level levelOverride) {
      this.logger = logger;
      this.levelOverride = levelOverride;
    }

    public LogLevelOverride(Class logger, Level levelOverride) {
      this(logger.getName(), levelOverride);
    }

    public void apply() {
      Logger logger = (Logger) LoggerFactory.getLogger(this.logger);
      originalLevel = logger.getLevel();
      logger.setLevel(levelOverride);
    }

    public void reset() {
      ((Logger) LoggerFactory.getLogger(this.logger)).setLevel(originalLevel);
    }
  }
}
