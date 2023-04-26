package com.github.kagkarlsson.scheduler;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;

public class StringUtilsTest {

  @Test
  public void test_truncate() {
    assertThat(StringUtils.truncate(null, 10), CoreMatchers.nullValue());
    assertThat(StringUtils.truncate("", 4), is(""));
    assertThat(StringUtils.truncate("1234", 4), is("1234"));
    assertThat(StringUtils.truncate("1234", 3), is("123"));
  }
}
