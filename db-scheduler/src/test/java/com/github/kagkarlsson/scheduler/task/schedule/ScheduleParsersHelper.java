package com.github.kagkarlsson.scheduler.task.schedule;

import static java.time.LocalTime.MIDNIGHT;
import static java.time.LocalTime.NOON;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

class ScheduleParsersHelper {
  private ScheduleParsersHelper() {}

  static final String ANY_SCHEDULE_STRING = "ANY STRING";
  static final Schedule FIXED_DELAY = FixedDelay.of(Duration.ZERO);
  static final Schedule DAILY_AT_MIDNIGHT = new Daily(MIDNIGHT);
  static final Schedule DAILY_AT_NOON = new Daily(NOON);

  static final Parser FIXED_DELAY_PARSER = new FakeParser(FIXED_DELAY);
  static final Parser DAILY_PARSER = new FakeParser(DAILY_AT_MIDNIGHT);
  static final Parser THROWING_PARSER =
      new FakeParser(
          () -> {
            throw new IllegalArgumentException("Error!");
          });

  static void assertUnrecognizableSchedule(Parser parser, String schedule) {
    try {
      parser.parse(schedule);
      fail("Should have thrown UnrecognizableSchedule for schedule '" + schedule + "'");
    } catch (Schedules.UnrecognizableSchedule e) {
      assertThat(e.getMessage(), containsString("Unrecognized schedule"));
      assertThat(e.getMessage(), containsString(parser.examples().toString()));
    }
  }

  static void assertSchedulePresent(Parser parser, String schedule) {
    try {
      assertTrue(parser.parse(schedule).isPresent());
    } catch (Exception e) {
      fail(
          "Should not have thrown any Exception for schedule '"
              + schedule
              + "'. Exception: "
              + e.getMessage());
    }
  }

  static void assertScheduleNotPresent(Parser parser, String schedule) {
    try {
      assertFalse(parser.parse(schedule).isPresent());
    } catch (Exception e) {
      fail(
          "Should not have thrown any Exception for schedule '"
              + schedule
              + "'. Exception: "
              + e.getMessage());
    }
  }

  static class FakeParser implements Parser {
    private final Supplier<Schedule> result;
    private final String example;

    FakeParser(Schedule result) {
      this(() -> result);
    }

    FakeParser(Supplier<Schedule> result) {
      this(result, "TEST");
    }

    public FakeParser(Supplier<Schedule> result, String example) {
      this.result = result;
      this.example = example;
    }

    @Override
    public Optional<Schedule> parse(String scheduleString) {
      return Optional.ofNullable(result.get());
    }

    @Override
    public List<String> examples() {
      return Collections.singletonList(example);
    }

    static Parser withMatchingSchedule(Schedule result) {
      return new FakeParser(result);
    }

    static Parser nonMatching() {
      return new FakeParser(() -> null);
    }
  }
}
