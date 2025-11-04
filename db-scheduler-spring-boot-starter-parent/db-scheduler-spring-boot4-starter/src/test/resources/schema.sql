-- DB Scheduler Schema for HSQLDB Test Database
CREATE TABLE scheduled_execution (
  task_name VARCHAR(100) NOT NULL,
  task_id VARCHAR(100) NOT NULL,
  exec_state VARCHAR(20) NOT NULL,
  picked_by VARCHAR(100),
  picked_at TIMESTAMP,
  data BYTEA,
  execution_data BYTEA,
  task_data BYTEA,
  PRIMARY KEY (task_name, task_id)
);