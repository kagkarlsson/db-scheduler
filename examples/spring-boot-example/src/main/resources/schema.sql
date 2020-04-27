--
-- Copyright (C) Gustav Karlsson
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

-- Table definition for HSQLDB.
-- Will get executed automatically by Spring Boot if the database is of type embedded.
create table if not exists scheduled_tasks (
	task_name varchar(100),
	task_instance varchar(100),
	task_data blob,
	execution_time TIMESTAMP WITH TIME ZONE,
	picked BIT,
	picked_by varchar(50),
	last_success TIMESTAMP WITH TIME ZONE,
	last_failure TIMESTAMP WITH TIME ZONE,
	consecutive_failures INT,
	last_heartbeat TIMESTAMP WITH TIME ZONE,
	version BIGINT,
	PRIMARY KEY (task_name, task_instance)
);
