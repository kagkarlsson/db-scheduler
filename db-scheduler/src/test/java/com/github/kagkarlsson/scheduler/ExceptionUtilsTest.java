package com.github.kagkarlsson.scheduler;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import java.io.IOException;
import org.junit.jupiter.api.Test;

class ExceptionUtilsTest {

  @Test
  void exceptionWithNoMessageDescribedAsItsClassSimpleName() {
    assertThat(
        ExceptionUtils.describe(new IllegalArgumentException()), is("IllegalArgumentException"));
  }

  @Test
  void exceptionDescribedAsClassSimpleNameAndMessage() {
    assertThat(
        ExceptionUtils.describe(new IOException("timed out")), is("IOException: 'timed out'"));
  }

  @Test
  void describingNullIsNull() {
    assertThat(ExceptionUtils.describe(null), is(nullValue()));
  }
}
