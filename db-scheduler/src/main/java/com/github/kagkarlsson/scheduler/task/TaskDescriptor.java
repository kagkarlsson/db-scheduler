package com.github.kagkarlsson.scheduler.task;

public interface TaskDescriptor<T> extends HasTaskName {

    String getTaskName();
    Class<T> getDataClass();

    static <T> TaskDescriptor<T> of(String name, Class<T> dataClass) {
        return new TaskDescriptor.SimpleTaskDescriptor<T>(name, dataClass);
    }

    class SimpleTaskDescriptor<T> implements TaskDescriptor<T> {

        private final String name;
        private final Class<T> dataClass;

        public SimpleTaskDescriptor(String name, Class<T> dataClass) {
            this.name = name;
            this.dataClass = dataClass;
        }

        @Override
        public String getTaskName() {
            return name;
        }

        @Override
        public Class<T> getDataClass() {
            return dataClass;
        }
    }
}
