create table scheduled_tasks (
  task_name varchar(100),
  task_instance varchar(100),
  execution_time timestamp with time zone,
  picked BOOLEAN,
  picked_by varchar(50),
  last_success timestamp with time zone,
  last_failure timestamp with time zone,
  last_heartbeat timestamp with time zone,
  version BIGINT,
  complete boolean DEFAULT false, 
  PRIMARY KEY (task_name, task_instance)
)