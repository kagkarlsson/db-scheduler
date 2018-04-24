create table scheduled_tasks (
  task_name varchar(250) not null,
  task_instance varchar(250) not null,
  task_data  nvarchar(max),
  execution_time datetimeoffset  not null,
  picked varchar(10),
  picked_by text,
  last_success datetimeoffset ,
  last_failure datetimeoffset ,
  last_heartbeat datetimeoffset ,
  [version] BIGINT not null,
  PRIMARY KEY (task_name, task_instance)
)