import com.github.dbunit.rules.DBUnitRule;
import com.github.dbunit.rules.api.dataset.DataSet;
import com.github.dbunit.rules.api.dataset.ExpectedDataSet;
import com.github.dbunit.rules.api.dbunit.DBUnitConfig;

import org.dbunit.database.DatabaseConfig;
import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.impl.DSL;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.jooq.example.flyway.db.h2.Tables.AUTHOR;
import static org.jooq.example.flyway.db.h2.Tables.BOOK;
import static org.junit.Assert.assertEquals;

/**
 * Created by Lukas on 23.06.2014.
 */
@DBUnitConfig(qualifiedTableNames = true)
public class JooqDBUnitTest {

    private static String DB_URL = "jdbc:h2:" + Paths.get("target").toAbsolutePath().toString() +
            "/flyway-test";

    private static Flyway flyway;


    private static Connection connection;

    @Rule
    public DBUnitRule dbUnitRule = DBUnitRule.
            instance(() -> flyway.getDataSource().getConnection());


    @BeforeClass
    public static void initMigration() throws SQLException {
        flyway = new Flyway();
        flyway.setDataSource(DB_URL, "sa", "");
        flyway.setLocations("filesystem:src/main/resources/db/migration");
        flyway.migrate();

        connection = flyway.getDataSource().getConnection();
        //add some data to test db cleanup
        try (Statement stmt = connection.createStatement()) {
            stmt.addBatch("INSERT INTO flyway_test.author(id, first_name, last_name, date_of_birth, year_of_birth, address) VALUES (1, 'Erich', 'Gamma','1903-06-25','1900',null)");
            stmt.addBatch("INSERT INTO flyway_test.author(id, first_name, last_name, date_of_birth, year_of_birth, address) VALUES (2, 'Richard', 'Helm','1903-06-25','1900',null)");
            int[] result = stmt.executeBatch();
            assertEquals(result.length, 2);
        }
    }

    @AfterClass
    public static void cleanMigration() throws SQLException {
        flyway.clean();
        if (!connection.isClosed()) {
            connection.close();
        }
    }

    @Test
    @DataSet("authors.yml,books.yml")
    public void shouldListAuthorsAndBooks() {
        Result<?> result =
                DSL.using(connection)
                        .select(
                                AUTHOR.FIRST_NAME,
                                AUTHOR.LAST_NAME,
                                BOOK.ID,
                                BOOK.TITLE
                        )
                        .from(AUTHOR)
                        .join(BOOK)
                        .on(AUTHOR.ID.eq(BOOK.AUTHOR_ID))
                        .orderBy(BOOK.ID.asc())
                        .fetch();

        assertEquals(4, result.size());
    }


    @Test
    @DataSet(cleanBefore = true, tableOrdering = {"flyway_test.book", "flyway_test.author"})
    public void shouldClearDataBaseUsingSequenceOrder() throws Exception {
        int size = countAuthors() + countBooks();
        assertEquals(0, size);
    }


    @Test
    @DataSet(cleanBefore = true, disableConstraints = true)
    public void shouldClearDataBaseDisablingConstraints() throws Exception {
        DSLContext dsl = DSL.using(connection);
        int size = dsl.fetchCount(AUTHOR);
        assertEquals(0, size);
    }

    @Test
    @DataSet("empty.yml")
    public void shouldClearDataBaseUsingEmptyDataSet() throws Exception {
        DSLContext dsl = DSL.using(connection);
        int size = dsl.fetchCount(AUTHOR);
        assertEquals(0, size);
    }

    @Test
    @DataSet("empty.yml")
    @ExpectedDataSet(value = "expected_authors.yml", ignoreCols = "id")
    public void shouldInsertAuthorsAndCompareDatabaseWithExpectedDataSet() throws Exception {
        assertEquals(0, countAuthors());
        try (Statement stmt = connection.createStatement()) {
            stmt.addBatch("INSERT INTO flyway_test.author(id, first_name, last_name, date_of_birth, year_of_birth, address) VALUES (1, 'Erich', 'Gamma','1903-06-25','1900',null)");
            stmt.addBatch("INSERT INTO flyway_test.author(id, first_name, last_name, date_of_birth, year_of_birth, address) VALUES (2, 'Richard', 'Helm','1903-06-25','1900',null)");
            int[] result = stmt.executeBatch();
            assertEquals(result.length, 2);
        }
        assertEquals(2, countAuthors());

    }

    public static int countAuthors() {
        return DSL.using(connection).fetchCount(AUTHOR);
    }

    public static int countBooks() {
        return DSL.using(connection).fetchCount(BOOK);
    }
}
