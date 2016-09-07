package com.github.dbunit.rules.leak;

import com.github.dbunit.rules.api.leak.LeakHunter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by pestano on 07/09/16.
 */
class OracleLeakHunter extends AbstractLeakHunter {


    private final String sql = "SELECT COUNT(*) FROM v$session WHERE status = 'INACTIVE'";

    Connection connection;

    public OracleLeakHunter(Connection connection){
        this.connection = connection;
    }

    @Override
    protected String leakCountSql() {
        return sql;
    }

    @Override
    protected Connection getConnection() {
        return connection;
    }
}
