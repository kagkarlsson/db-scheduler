package com.github.kagkarlsson.scheduler.exceptions;

public class TaskInstanceCurrentlyRunningException extends RuntimeException{
    public TaskInstanceCurrentlyRunningException(String message){
        super(message);
    }
}
