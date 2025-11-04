DROP TABLE scheduled_tasks IF EXISTS;

create table scheduled_tasks (
	task_name varchar(100) not null,
	task_instance varchar(100) not null,
	task_data blob,
	execution_time TIMESTAMP WITH TIME ZONE,
	picked boolean,
	picked_by varchar(50),
	last_success TIMESTAMP WITH TIME ZONE,
	last_failure TIMESTAMP WITH TIME ZONE,
	consecutive_failures integer,
	last_heartbeat TIMESTAMP WITH TIME ZONE,
	version bigint,
	priority integer,
	PRIMARY KEY (task_name, task_instance)
);
