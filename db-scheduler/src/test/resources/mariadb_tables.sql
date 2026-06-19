-- Best-effort schema definition based on MySQL schema. Please suggest improvements.
create table test.scheduled_tasks (
  task_name varchar(100) not null,
  task_instance varchar(100) not null,
  task_data blob,
  execution_time datetime(6) not null,
  picked BOOLEAN not null,
  picked_by varchar(50),
  last_success datetime(6) null,
  last_failure datetime(6) null,
  consecutive_failures INT,
  last_heartbeat datetime(6) null,
  version BIGINT not null,
  priority SMALLINT,
  PRIMARY KEY (task_name, task_instance),
  INDEX execution_time_idx (execution_time),
  INDEX last_heartbeat_idx (last_heartbeat),
  INDEX priority_execution_time_idx (priority desc, execution_time asc)
)

-- an optimization for users of priority might be to add priority to the execution_time_idx
-- this _might_ save reads as the priority-value is already in the index
-- INDEX priority_execution_time_idx (execution_time asc, priority desc)
