create table scheduled_tasks (
  task_name varchar(40) not null,
  task_instance varchar(40) not null,
  task_data blob,
  execution_time timestamp(6) not null,
  picked BOOLEAN not null,
  picked_by varchar(40),
  last_success timestamp(6) null,
  last_failure timestamp(6) null,
  last_heartbeat timestamp(6) null,
  version BIGINT not null,
  PRIMARY KEY (task_name, task_instance)
)