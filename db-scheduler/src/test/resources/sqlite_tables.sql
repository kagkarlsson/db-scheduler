create table scheduled_tasks (
    task_name varchar(100),
    task_instance varchar(100),
    task_data blob,
    execution_time TIMESTAMP,
    picked BIT,
    picked_by varchar(50),
    last_success TIMESTAMP,
    last_failure TIMESTAMP,
    consecutive_failures INT,
    last_heartbeat TIMESTAMP,
    version BIGINT,
    priority SMALLINT,
    PRIMARY KEY (task_name, task_instance)
);

CREATE INDEX execution_time_idx ON scheduled_tasks (execution_time);
CREATE INDEX last_heartbeat_idx ON scheduled_tasks (last_heartbeat);
CREATE INDEX priority_execution_time_idx on scheduled_tasks (priority desc, execution_time asc);
