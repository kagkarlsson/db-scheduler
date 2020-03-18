create table scheduled_tasks (
    task_name varchar(100),
    task_instance varchar(100),
    task_data blob,
    execution_time TIMESTAMP(6),
    picked NUMBER(1,0),
    picked_by varchar(50),
    last_success TIMESTAMP(6),
    last_failure TIMESTAMP(6),
    consecutive_failures NUMBER(19,0),
  last_heartbeat TIMESTAMP(6),
    version NUMBER(19,0),
    PRIMARY KEY (task_name, task_instance)
);
