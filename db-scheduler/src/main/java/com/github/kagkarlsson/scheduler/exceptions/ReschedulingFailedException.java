package com.github.kagkarlsson.scheduler.exceptions;

public class ReschedulingFailedException extends RuntimeException{
    public ReschedulingFailedException(String message){
        super(message);
    }
}
