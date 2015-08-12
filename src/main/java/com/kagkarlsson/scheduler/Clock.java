package com.kagkarlsson.scheduler;

import java.time.LocalDateTime;

public interface Clock {
	LocalDateTime now();
}
