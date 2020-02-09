package com.github.kagkarlsson.scheduler.task.schedule;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static java.time.LocalTime.MIDNIGHT;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

class ScheduleParsersHelper {
    private ScheduleParsersHelper() {
    }

    static final String ANY_SCHEDULE_STRING = "ANY STRING";
    static final Schedule FIXED_DELAY = FixedDelay.of(Duration.ZERO);
    static final Schedule DAILY = new Daily(MIDNIGHT);

    static final Parser FIXED_DELAY_PARSER = new FakeParser(FIXED_DELAY);
    static final Parser DAILY_PARSER = new FakeParser(DAILY);
    static final Parser THROWING_PARSER = new FakeParser(() -> {
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

    static void assertParsableSchedule(Parser parser, String schedule) {
        try {
            parser.parse(schedule);
        } catch (Exception e) {
            fail("Should not have thrown any Exception for schedule '" + schedule + "'. Exception: " + e.getMessage());
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
        public Schedule parse(String scheduleString) {
            return result.get();
        }

        @Override
        public List<String> examples() {
            return Collections.singletonList(example);
        }
    }
}
