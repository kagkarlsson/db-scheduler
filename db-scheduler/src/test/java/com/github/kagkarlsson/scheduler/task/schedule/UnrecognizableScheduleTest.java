package com.github.kagkarlsson.scheduler.task.schedule;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

public class UnrecognizableScheduleTest {
    @Test
    public void contains_correct_message() {
        assertMessage(null, "Unrecognized schedule: 'null'");
        assertMessage("Wrong", "Unrecognized schedule: 'Wrong'");
        assertMessage(null, Arrays.asList("PARSABLE 1", "PARSABLE 2"),
                "Unrecognized schedule: 'null'. Parsable examples: [PARSABLE 1, PARSABLE 2]");
    }

    private static void assertMessage(String scheduleString, String expectedMessage) {
        assertEquals(expectedMessage, new Schedules.UnrecognizableSchedule(scheduleString).getMessage());
    }

    private static void assertMessage(String scheduleString, List<String> examples, String expectedMessage) {
        assertEquals(expectedMessage, new Schedules.UnrecognizableSchedule(scheduleString, examples).getMessage());
    }
}
