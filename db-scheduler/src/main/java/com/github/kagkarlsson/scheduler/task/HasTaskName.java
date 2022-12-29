package com.github.kagkarlsson.scheduler.task;

public interface HasTaskName {
    String getTaskName();

    static HasTaskName of(String name) {
        return new SimpleTaskName(name);
    }

    class SimpleTaskName implements HasTaskName {
        private String name;

        public SimpleTaskName(String name) {

            this.name = name;
        }

        @Override
        public String getTaskName() {
            return name;
        }
    }
}
