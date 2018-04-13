package com.github.kagkarlsson.scheduler;

public interface SchedulingEventListener {

    void newEvent(SchedulingEvent event);


    SchedulingEventListener NOOP = new SchedulingEventListener() {

        @Override
        public void newEvent(SchedulingEvent event) {
        }
    };
}
