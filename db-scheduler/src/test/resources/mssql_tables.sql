create table scheduled_tasks
(
  task_name            varchar(250)   not null,
  task_instance        varchar(250)   not null,
  task_data            nvarchar(max),
  execution_time       datetimeoffset not null,
  picked               bit,
  picked_by            varchar(50),
  last_success         datetimeoffset,
  last_failure         datetimeoffset,
  consecutive_failures int,
  last_heartbeat       datetimeoffset,
  [version]            bigint         not null,
  priority             int            not null,
  primary key (task_name, task_instance),
  index execution_time_idx (execution_time),
  index last_heartbeat_idx (last_heartbeat),
  index priority_execution_time_idx (priority desc, execution_time asc)
)
