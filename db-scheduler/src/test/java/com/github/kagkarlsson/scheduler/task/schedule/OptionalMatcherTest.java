package com.github.kagkarlsson.scheduler.task.schedule;

import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.util.Optional;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;

import static org.junit.Assert.*;

public class OptionalMatcherTest {
    private final OptionalMatcher matcher = OptionalMatcher.from(Pattern.compile("Matches"));
	@Test
	public void should_fail_creating_from_null_pattern() {
        try {
            OptionalMatcher.from(null);
            fail("Should have thrown NullPointerException for pattern '" + null + "'");
        } catch (NullPointerException e) {
            assertThat(e.getMessage(), CoreMatchers.containsString("A non null pattern must be specified"));
        }
	}

    @Test
    public void should_not_fail_parsing_null() {
        Optional<MatchResult> result = matcher.match(null);
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void should_return_empty_optional_for_non_matching_string() {
        Optional<MatchResult> result = matcher.match("Non matching");
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void should_return_not_empty_optional_for_matching_string() {
        Optional<MatchResult> result = matcher.match("Matches");
        assertTrue(result.isPresent());
    }
}
