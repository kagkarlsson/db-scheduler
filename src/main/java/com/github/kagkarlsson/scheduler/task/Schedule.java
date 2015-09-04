package com.github.kagkarlsson.scheduler.task;

import java.time.LocalDateTime;

public interface Schedule {
	LocalDateTime getNextExecutionTime(LocalDateTime timeDone);
}
