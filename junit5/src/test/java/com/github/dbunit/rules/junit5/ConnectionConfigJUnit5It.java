package com.github.dbunit.rules.junit5;

import com.github.dbunit.rules.DBUnitRule;
import com.github.dbunit.rules.api.configuration.DBUnit;
import com.github.dbunit.rules.api.dataset.DataSet;
import com.github.dbunit.rules.junit5.model.User;
import com.github.dbunit.rules.util.EntityManagerProvider;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import static com.github.dbunit.rules.util.EntityManagerProvider.em;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by rafael-pestano on 13/09/2016.
 */
@ExtendWith(DBUnitExtension.class) //<1>
@RunWith(JUnitPlatform.class) //<2>
@DBUnit(url = "jdbc:hsqldb:mem:junit5;DB_CLOSE_DELAY=-1", driver = "org.hsqldb.jdbcDriver", user = "sa")
public class ConnectionConfigJUnit5It {



    @BeforeAll
    public static void initDB(){
        //trigger db creation
        EntityManagerProvider.instance("junit5-pu");
    }

    @Test
    @DataSet(value = "users.yml")
    public void shouldSeedFromDeclaredConnection() {
        User user = (User) em().createQuery("select u from User u where u.id = 1").getSingleResult();
        assertThat(user).isNotNull();
        assertThat(user.getId()).isEqualTo(1);
    }
}
