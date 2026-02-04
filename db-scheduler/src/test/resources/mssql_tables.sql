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
  priority             smallint,
  primary key (task_instance, task_name),
  index execution_time_idx (execution_time),
  index last_heartbeat_idx (last_heartbeat),
  index priority_execution_time_idx (priority desc, execution_time asc)
)

-- an optimization for users of priority might be to add priority to the priority_execution_time_idx
-- this _might_ save reads as the priority-value is already in the index
-- index priority_execution_time_idx (execution_time asc, priority desc)

-- Migrations
ALTER TABLE scheduled_tasks ADD state varchar(20);
CREATE INDEX state_execution_time_idx ON scheduled_tasks (state, execution_time);
