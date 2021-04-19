package com.github.kagkarlsson.scheduler.exceptions;

public class ExecutionException extends RuntimeException {
    public ExecutionException(String message){
        super(message);
    }
    public ExecutionException(String message, Exception e){
        super(message, e);
    }
}
