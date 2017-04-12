create table scheduled_tasks (
	task_name varchar(100),
	task_instance varchar(100),
	task_state text,
	execution_time TIMESTAMP(6),
	picked BIT,
	picked_by varchar(50),
	last_success TIMESTAMP(6),
	last_failure TIMESTAMP(6),
	last_heartbeat TIMESTAMP(6),
	version BIGINT,
    complete BIT DEFAULT FALSE,
	PRIMARY KEY (task_name, task_instance)
)