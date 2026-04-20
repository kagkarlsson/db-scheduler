-- When using TIMESTAMP without time zone, always use .alwaysPersistTimestampInUTC() setting
--  to ensure correct handling of instants.

create table scheduled_tasks
(
    task_name            varchar(100),
    task_instance        varchar(100),
    task_data            blob,
    execution_time       TIMESTAMP(6),
    picked               NUMBER(1, 0),
    picked_by            varchar(50),
    last_success         TIMESTAMP(6),
    last_failure         TIMESTAMP(6),
    consecutive_failures NUMBER(19, 0),
    last_heartbeat       TIMESTAMP(6),
    version              NUMBER(19, 0),
    priority             SMALLINT,
    PRIMARY KEY (task_name, task_instance)
);

CREATE INDEX scheduled_tasks_execution_time_idx on scheduled_tasks(execution_time);
CREATE INDEX scheduled_tasks_last_heartbeat_idx on scheduled_tasks(last_heartbeat);
CREATE INDEX scheduled_tasks_priority_execution_time_idx on scheduled_tasks(priority desc, execution_time asc);

