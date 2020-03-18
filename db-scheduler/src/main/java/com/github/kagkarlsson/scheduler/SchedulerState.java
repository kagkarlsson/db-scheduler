/**
 * Copyright (C) Gustav Karlsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.scheduler;

public interface SchedulerState {

	boolean isShuttingDown();

	boolean isStarted();

	class SettableSchedulerState implements SchedulerState {

		private boolean isShuttingDown;
		private boolean isStarted;

		@Override
		public boolean isShuttingDown() {
			return isShuttingDown;
		}

		@Override
		public boolean isStarted() {
			return isStarted;
		}

		public void setIsShuttingDown() {
			this.isShuttingDown = true;
		}

		public void setStarted() {
			this.isStarted = true;
		}
	}

}
