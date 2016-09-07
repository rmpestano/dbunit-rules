package com.github.dbunit.rules.leak;

import com.github.dbunit.rules.api.leak.LeakHunter;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.github.dbunit.rules.util.DriverUtils.*;

/**
 * Created by pestano on 07/09/16.
 */
public class LeakHunterFactory {

    private static final Logger LOG = Logger.getLogger(LeakHunterFactory.class.getName());

    public static LeakHunter from(Connection connection) {
        try {
            if (connection == null || connection.isClosed()) {
                throw new RuntimeException("Cannot create Leak Hunter from a null or closed connection");
            }
        } catch (SQLException e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
        }
        String driverName = getDriverName(connection);

        if (isHsql(driverName) || isH2(driverName)) {
            return new InMemoryLeakHunter(connection);
        } else if (isPostgre(driverName)) {
            return new PostgreLeakHunter(connection);
        } else if (isMysql(driverName)) {
            return new MySqlLeakHunter(connection);
        } else if (isOracle(driverName)) {
            return new OracleLeakHunter(connection);
        }
        return null;
    }
}
