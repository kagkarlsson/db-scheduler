create table custom_tablename (
  task_name text not null,
  task_instance text not null,
  task_data bytea,
  execution_time timestamp with time zone not null,
  picked BOOLEAN not null,
  picked_by text,
  last_success timestamp with time zone,
  last_failure timestamp with time zone,
  last_heartbeat timestamp with time zone,
  version BIGINT not null,
  PRIMARY KEY (task_name, task_instance)
)