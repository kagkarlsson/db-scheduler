package com.github.kagkarlsson.scheduler.task.schedule;

import static com.github.kagkarlsson.scheduler.task.schedule.ScheduleParsersHelper.ANY_SCHEDULE_STRING;
import static com.github.kagkarlsson.scheduler.task.schedule.ScheduleParsersHelper.DAILY_AT_MIDNIGHT;
import static com.github.kagkarlsson.scheduler.task.schedule.ScheduleParsersHelper.DAILY_AT_NOON;
import static com.github.kagkarlsson.scheduler.task.schedule.ScheduleParsersHelper.DAILY_PARSER;
import static com.github.kagkarlsson.scheduler.task.schedule.ScheduleParsersHelper.FIXED_DELAY_PARSER;
import static com.github.kagkarlsson.scheduler.task.schedule.ScheduleParsersHelper.THROWING_PARSER;
import static com.github.kagkarlsson.scheduler.task.schedule.ScheduleParsersHelper.assertScheduleNotPresent;
import static com.github.kagkarlsson.scheduler.task.schedule.ScheduleParsersHelper.assertSchedulePresent;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.github.kagkarlsson.scheduler.task.schedule.ScheduleParsersHelper.FakeParser;
import java.util.Optional;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CompositeParserTest {
  private final Parser MATCHING_PARSER_MIDNIGHT =
      FakeParser.withMatchingSchedule(DAILY_AT_MIDNIGHT);
  private final Parser MATCHING_PARSER_NOON = FakeParser.withMatchingSchedule(DAILY_AT_NOON);
  private final Parser NON_MATCHING_PARSER = FakeParser.nonMatching();
  private final Parser ANOTHER_NON_MATCHING_PARSER = FakeParser.nonMatching();

  @Test
  public void returns_schedule_from_the_first_matching_parser() {
    assertParsingResult(
        DAILY_AT_MIDNIGHT, MATCHING_PARSER_MIDNIGHT, MATCHING_PARSER_NOON, NON_MATCHING_PARSER);
    assertParsingResult(
        DAILY_AT_MIDNIGHT, MATCHING_PARSER_MIDNIGHT, NON_MATCHING_PARSER, MATCHING_PARSER_NOON);
    assertParsingResult(
        DAILY_AT_MIDNIGHT, MATCHING_PARSER_MIDNIGHT, THROWING_PARSER, MATCHING_PARSER_NOON);
    assertParsingResult(
        DAILY_AT_MIDNIGHT, NON_MATCHING_PARSER, MATCHING_PARSER_MIDNIGHT, MATCHING_PARSER_NOON);
    assertParsingResult(
        DAILY_AT_MIDNIGHT, NON_MATCHING_PARSER, MATCHING_PARSER_MIDNIGHT, THROWING_PARSER);
    assertParsingResult(
        DAILY_AT_NOON, MATCHING_PARSER_NOON, MATCHING_PARSER_MIDNIGHT, NON_MATCHING_PARSER);
    assertParsingResult(
        DAILY_AT_NOON, NON_MATCHING_PARSER, MATCHING_PARSER_NOON, MATCHING_PARSER_MIDNIGHT);
    assertParsingResult(DAILY_AT_NOON, MATCHING_PARSER_NOON, NON_MATCHING_PARSER, THROWING_PARSER);
  }

  @Test
  public void fails_when_no_successful_parsers() {
    assertScheduleNotPresent(
        CompositeParser.of(NON_MATCHING_PARSER, ANOTHER_NON_MATCHING_PARSER), ANY_SCHEDULE_STRING);
  }

  @Test
  public void fails_when_one_required_delegate_fails() {
    Assertions.assertThrows(
        Exception.class,
        () -> {
          CompositeParser.of(NON_MATCHING_PARSER, THROWING_PARSER, MATCHING_PARSER_MIDNIGHT)
              .parse(ANY_SCHEDULE_STRING);
        });
  }

  @Test
  public void cannot_create_from_null_or_empty_list() {
    assertUnableToCreateParser(null);
    assertUnableToCreateParser();
  }

  @Test
  public void examples_should_be_parsable() {
    Parser parser = CompositeParser.of(DAILY_PARSER, FIXED_DELAY_PARSER);
    parser.examples().forEach(it -> assertSchedulePresent(parser, it));
  }

  @Test
  public void examples_should_come_from_delegates() {
    Parser parser1 = new FakeParser(() -> DAILY_AT_MIDNIGHT, "PARSABLE 1");
    Parser parser2 = new FakeParser(() -> DAILY_AT_MIDNIGHT, "PARSABLE 2");
    Parser parser = CompositeParser.of(parser1, parser2);
    assertThat(parser.examples(), containsInAnyOrder("PARSABLE 1", "PARSABLE 2"));
  }

  static void assertParsingResult(Schedule expected, Parser... parsers) {
    assertEquals(Optional.of(expected), CompositeParser.of(parsers).parse(ANY_SCHEDULE_STRING));
  }

  static void assertUnableToCreateParser(Parser... parsers) {
    try {
      CompositeParser.of(parsers);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), CoreMatchers.containsString("Unable to create CompositeParser"));
    }
  }
}
