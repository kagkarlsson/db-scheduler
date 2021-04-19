package com.github.kagkarlsson.scheduler.exceptions;

public class TaskInstanceNotFoundException extends RuntimeException{
    public TaskInstanceNotFoundException(String message){
        super(message);
    }
}
