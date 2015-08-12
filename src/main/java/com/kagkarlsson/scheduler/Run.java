package com.kagkarlsson.scheduler;

import org.slf4j.Logger;

public class Run {

	public static Runnable logExceptions(Logger logger, Runnable runnable) {
		return () -> {
			try {
				runnable.run();
			} catch (RuntimeException e) {
				logger.error("Unexpected error.", e);
			}
		};
	}

}
