create table scheduled_tasks
(
    task_name            varchar(100),
    task_instance        varchar(100),
    task_data            blob,
    execution_time       TIMESTAMP(6) WITH TIME ZONE,
    picked               NUMBER(1, 0),
    picked_by            varchar(50),
    last_success         TIMESTAMP(6) WITH TIME ZONE,
    last_failure         TIMESTAMP(6) WITH TIME ZONE,
    consecutive_failures NUMBER(19, 0),
    last_heartbeat       TIMESTAMP(6) WITH TIME ZONE,
    version              NUMBER(19, 0),
    priority             SMALLINT,
    PRIMARY KEY (task_name, task_instance)
);

CREATE INDEX scheduled_tasks__execution_time__idx on scheduled_tasks(execution_time);
CREATE INDEX scheduled_tasks__last_heartbeat__idx on scheduled_tasks(last_heartbeat);
CREATE INDEX scheduled_tasks__priority__execution_time__idx on scheduled_tasks(priority desc, execution_time asc);

-- an optimization for users of priority might be to add priority to the scheduled_tasks__execution_time__idx
-- this _might_ save reads as the priority-value is already in the index
