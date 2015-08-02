package com.github.dbunit.rules.connection;

import com.github.dbunit.rules.api.connection.ConnectionHolder;

import java.sql.Connection;

/**
 * Created by pestano on 25/07/15.
 */
public class ConnectionHolderImpl implements ConnectionHolder {

    private Connection connection;

    public ConnectionHolderImpl(Connection connection) {
        this.connection = connection;
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

}
