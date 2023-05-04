package com.github.kagkarlsson.examples.boot.config;

import java.io.Serializable;

public class EmailData implements Serializable {
    private static final long serialVersionUID = 1L; // json serialization can also be used
    public final String username;
    public final String emailAddress;

    public EmailData(String username, String emailAddress) {
        this.username = username;
        this.emailAddress = emailAddress;
    }
}
