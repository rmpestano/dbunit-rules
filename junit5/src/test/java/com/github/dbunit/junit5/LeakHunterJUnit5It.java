package com.github.dbunit.junit5;

import com.github.dbunit.rules.api.configuration.DBUnit;
import com.github.dbunit.rules.api.connection.ConnectionHolder;
import com.github.dbunit.rules.api.dataset.DataSet;
import com.github.dbunit.rules.leak.LeakHunterException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.sql.*;

import static com.github.dbunit.rules.util.EntityManagerProvider.instance;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.expectThrows;

@ExtendWith(DBUnitExtension.class)
@RunWith(JUnitPlatform.class)
@DBUnit(activateLeakHunter = true)
@Disabled(value = "find a way to test leak exceptions")
public class LeakHunterJUnit5It {

    private ConnectionHolder connectionHolder = () ->
            instance("junit5-pu").connection();


    @Test
    @DataSet("users.yml")
    public void shouldFindConnectionLeak() throws SQLException {

        createLeak();

        Throwable exception = expectThrows(LeakHunterException.class, () -> {
        });
        assertThat(exception.getMessage()).isEqualTo("Execution of method shouldFindConnectionLeak left 1 open connection(s).");
    }

    @Test
    @DataSet("users.yml")
    public void shouldFindTwoConnectionLeaks() throws SQLException {
        createLeak();
        createLeak();
        Throwable exception = expectThrows(LeakHunterException.class, () -> {
        });
        assertThat(exception.getMessage()).isEqualTo("Execution of method shouldFindConnectionLeak left 1 open connection(s).");

    }


    @Test
    @DataSet("users.yml")
    @DBUnit(activateLeakHunter = false)
    public void shouldNotFindConnectionLeakWhenHunterIsDisabled() throws SQLException {
        createLeak();
    }

    @Test
    @DataSet("users.yml")
    public void shouldNotFindConnectionLeakWhenConnectionIsClosed() throws SQLException {
        createAndCloseConnection();
    }

    private Connection getConnection() {
        try {
            return DriverManager.getConnection("jdbc:hsqldb:mem:junit5;DB_CLOSE_DELAY=-1", "sa", "");
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    private void createLeak() throws SQLException {
        Connection connection = getConnection();
        try (Statement stmt = connection.createStatement()) {
            ResultSet resultSet = stmt.executeQuery("select count(*) from user");
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getInt(1)).isEqualTo(2);
        }
    }

    private void createAndCloseConnection() throws SQLException {
        Connection connection = getConnection();
        try (Statement stmt = connection.createStatement()) {
            ResultSet resultSet = stmt.executeQuery("select count(*) from user");
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getInt(1)).isEqualTo(2);
        } finally {
            connection.close();
        }
    }

}
