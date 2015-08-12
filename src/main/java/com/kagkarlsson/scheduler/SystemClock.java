package com.kagkarlsson.scheduler;

import java.time.LocalDateTime;

public class SystemClock implements Clock {

	@Override
	public LocalDateTime now() {
		return LocalDateTime.now();
	}
}
