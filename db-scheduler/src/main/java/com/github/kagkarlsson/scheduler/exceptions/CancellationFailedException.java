package com.github.kagkarlsson.scheduler.exceptions;

public class CancellationFailedException extends RuntimeException{
    public CancellationFailedException(String message){
        super(message);
    }
}
