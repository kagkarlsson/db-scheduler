package com.github.kagkarlsson.scheduler;

public interface StatsRegistry {

	void registerUnexpectedError();

	StatsRegistry NOOP = new StatsRegistry() {
		@Override
		public void registerUnexpectedError() {
		}
	};

}
