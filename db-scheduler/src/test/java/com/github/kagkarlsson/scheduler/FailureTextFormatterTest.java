package com.github.kagkarlsson.scheduler;

import static com.github.kagkarlsson.scheduler.FailureTextFormatter.MESSAGE_ONLY;
import static com.github.kagkarlsson.scheduler.FailureTextFormatter.STACK_TRACE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.PrintWriter;
import java.io.StringWriter;
import org.junit.jupiter.api.Test;

class FailureTextFormatterTest {

  @Test
  void stack_trace_is_same_as_printStackTrace_when_it_fits() {
    var e = nested(5, () -> new RuntimeException("outer", new IllegalStateException("root")));

    assertThat(STACK_TRACE.format(e, 100_000)).isEqualTo(printStackTrace(e));
  }

  @Test
  void stack_trace_trims_frames_per_exception_but_keeps_every_exception() {
    var root = nested(60, () -> new IllegalStateException("root"));
    var e = nested(60, () -> new RuntimeException("outer", root));
    var full = printStackTrace(e);

    var text = STACK_TRACE.format(e, 2000);

    assertThat(full.length()).isGreaterThan(4000);
    assertThat(text.length()).isLessThanOrEqualTo(2000);
    assertThat(text)
        .startsWith("java.lang.RuntimeException: outer\n\tat ")
        .contains("\nCaused by: java.lang.IllegalStateException: root\n\tat ")
        .containsPattern("\n\t\\.\\.\\. \\d+ more\nCaused by:")
        .endsWith(" more");
    // the line where each exception was thrown is kept
    assertThat(text).contains("\tat " + e.getStackTrace()[0]);
    assertThat(text).contains("\tat " + root.getStackTrace()[0]);
  }

  @Test
  void stack_trace_drops_all_frames_before_dropping_an_exception() {
    var root = new IllegalStateException("root");
    var e = nested(30, () -> new RuntimeException("outer", root));
    var headersOnly = e + "\nCaused by: " + root;
    var withOneFrame = STACK_TRACE.format(e, headersOnly.length() + 400);

    assertThat(STACK_TRACE.format(e, headersOnly.length())).isEqualTo(headersOnly);
    assertThat(withOneFrame).contains("\tat " + e.getStackTrace()[0]);
    assertThat(withOneFrame.length()).isLessThanOrEqualTo(headersOnly.length() + 400);
  }

  @Test
  void suppressed_exceptions_are_listed_without_frames() {
    var e = new RuntimeException("outer");
    e.addSuppressed(new IllegalStateException("closing failed"));

    assertThat(STACK_TRACE.format(e, 100_000))
        .contains("\n\tSuppressed: java.lang.IllegalStateException: closing failed")
        .doesNotContain("\t\tat ");
  }

  @Test
  void message_only_lists_each_exception_in_the_chain() {
    var e = new RuntimeException("outer", new IllegalStateException("root"));

    assertThat(MESSAGE_ONLY.format(e, 100_000))
        .isEqualTo(
            "java.lang.RuntimeException: outer\n"
                + "Caused by: java.lang.IllegalStateException: root");
  }

  @Test
  void cyclic_cause_chain_terminates() {
    var a = new RuntimeException("a");
    var b = new RuntimeException("b", a);
    a.initCause(b);

    assertThat(MESSAGE_ONLY.format(a, 100_000))
        .isEqualTo("java.lang.RuntimeException: a\n" + "Caused by: java.lang.RuntimeException: b");
  }

  @Test
  void failure_texts_cuts_to_max_length_and_falls_back_when_formatter_throws() {
    var e = new RuntimeException("boom");

    var cut = new FailureTexts((t, max) -> "x".repeat(100), 50).format(e);
    assertThat(cut).hasSize(50).endsWith(FailureTexts.TRUNCATED_MARKER);

    var emoji = "😀".repeat(50); // surrogate pairs must not be split
    assertThat(FailureTexts.truncate(emoji, 50))
        .hasSize(49)
        .startsWith("😀")
        .endsWith("😀" + FailureTexts.TRUNCATED_MARKER);

    var fallback =
        new FailureTexts(
                (t, max) -> {
                  throw new IllegalStateException("formatter bug");
                },
                4000)
            .format(e);
    assertThat(fallback).isEqualTo("java.lang.RuntimeException");

    assertThat(FailureTexts.truncate("a\u0000b", 50)).isEqualTo("a?b");

    assertThat(new FailureTexts(FailureTextFormatter.NONE, 4000).format(e)).isNull();
    assertThatThrownBy(() -> new FailureTexts(STACK_TRACE, 5))
        .isInstanceOf(IllegalArgumentException.class);
  }

  private static <T extends Throwable> T nested(int depth, java.util.function.Supplier<T> create) {
    return depth == 0 ? create.get() : nested(depth - 1, create);
  }

  private static String printStackTrace(Throwable e) {
    var sw = new StringWriter();
    e.printStackTrace(new PrintWriter(sw));
    return sw.toString().replace(System.lineSeparator(), "\n").stripTrailing();
  }
}
