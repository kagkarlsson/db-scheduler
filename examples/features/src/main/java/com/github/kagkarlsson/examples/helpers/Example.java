package com.github.kagkarlsson.examples.helpers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

public abstract class Example {

    protected Logger log = LoggerFactory.getLogger(getClass());

    public abstract void run(DataSource ds);

    protected void runWithDatasource() {
        run(HsqlDatasource.initDatabase());
    }
}
