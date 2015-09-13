create table scheduled_tasks (
  task_name varchar(100),
  task_instance varchar(100),
  execution_time TIMESTAMP(6),
  picked BOOLEAN,
  picked_by varchar(50),
  last_heartbeat TIMESTAMP(6),
  version BIGINT,
  PRIMARY KEY (task_name, task_instance)
)