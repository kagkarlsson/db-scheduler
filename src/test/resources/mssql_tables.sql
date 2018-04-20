create table scheduled_tasks (
  task_name varchar(250) not null,
  task_instance varchar(250) not null,
  task_data datetime,
  execution_time datetime not null,
  picked varchar(10),
  picked_by text,
  last_success datetime,
  last_failure datetime,
  last_heartbeat datetime,
  [version] BIGINT not null,
  PRIMARY KEY (task_name, task_instance)
)