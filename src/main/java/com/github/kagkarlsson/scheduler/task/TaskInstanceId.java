package com.github.kagkarlsson.scheduler.task;

public interface TaskInstanceId {
    String getTaskName();
    String getId();

    class StandardTaskInstanceId implements TaskInstanceId {
        private final String taskName;
        private final String id;

        public StandardTaskInstanceId(String taskName, String id) {
            this.taskName = taskName;
            this.id = id;
        }

        @Override
        public String getTaskName() {
            return this.taskName;
        }

        @Override
        public String getId() {
            return this.id;
        }
    }
}
