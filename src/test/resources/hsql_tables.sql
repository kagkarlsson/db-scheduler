create table scheduled_tasks (
	task_name varchar(100),
	task_instance varchar(100),
	execution_time TIMESTAMP(6),
	picked BIT,
	PRIMARY KEY (task_name, task_instance)
)