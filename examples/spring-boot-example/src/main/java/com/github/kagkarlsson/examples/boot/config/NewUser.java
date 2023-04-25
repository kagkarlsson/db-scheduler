package com.github.kagkarlsson.examples.boot.config;

import java.io.Serializable;

public class NewUser implements Serializable {
    private static final long serialVersionUID = 1L; // json serialization can also be used
    private final String username;
    private final String emailAddress;

    public NewUser(String username, String emailAddress) {
        this.username = username;
        this.emailAddress = emailAddress;
    }
}
