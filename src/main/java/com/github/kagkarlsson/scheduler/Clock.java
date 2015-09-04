package com.github.kagkarlsson.scheduler;

import java.time.LocalDateTime;

public interface Clock {
	LocalDateTime now();
}
