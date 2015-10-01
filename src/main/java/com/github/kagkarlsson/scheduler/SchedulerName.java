/**
 * Copyright (C) Gustav Karlsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

public interface SchedulerName {

	String getName();


	class Fixed implements SchedulerName {
		private final String name;

		public Fixed(String name) {
			this.name = name;
		}

		@Override
		public String getName() {
			return name;
		}
	}


	class Hostname implements SchedulerName {
		private static final Logger LOG = LoggerFactory.getLogger(Hostname.class);

		@Override
		public String getName() {
			try {
				return InetAddress.getLocalHost().getHostName();
			} catch (UnknownHostException e) {
				LOG.warn("Failed to resolve hostname. Using dummy-name for scheduler.");
				return "failed.hostname.lookup";
			}
		}
	}
}
