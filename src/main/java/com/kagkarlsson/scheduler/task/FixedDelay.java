package com.kagkarlsson.scheduler.task;

import java.time.Duration;
import java.time.LocalDateTime;

public class FixedDelay implements Schedule {

	private final Duration duration;

	private FixedDelay(Duration duration) {
		this.duration = duration;
	}

	public static FixedDelay of(Duration duration) {
		return new FixedDelay(duration);
	}

	@Override
	public LocalDateTime getNextExecutionTime(LocalDateTime from) {
		return from.plus(duration);
	}
}
