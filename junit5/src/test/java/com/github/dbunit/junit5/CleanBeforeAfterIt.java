package com.github.dbunit.junit5;

import com.github.dbunit.junit5.model.User;
import com.github.dbunit.rules.api.connection.ConnectionHolder;
import com.github.dbunit.rules.api.dataset.DataSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.util.List;

import static com.github.dbunit.rules.util.EntityManagerProvider.em;
import static com.github.dbunit.rules.util.EntityManagerProvider.instance;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Created by pestano on 26/02/16.
 */
@ExtendWith(DBUnitExtension.class)
@RunWith(JUnitPlatform.class)
public class CleanBeforeAfterIt {

    private ConnectionHolder connectionHolder = () ->
            instance("junit5-pu").connection();


    @BeforeAll
    public static void before(){
        em("junit5-pu").getTransaction().begin();
        em().createNativeQuery("DELETE FROM USER").executeUpdate();
        em().createNativeQuery("INSERT INTO USER VALUES (6,'user6')").executeUpdate();
        em().flush();
        em().getTransaction().commit();
        List<User> users = em().createQuery("select u from User u").getResultList();
        assertThat(users).isNotNull().hasSize(1);

    }

    @AfterAll
    public static void after(){
        List<User> users = em().createQuery("select u from User u").getResultList();
        if(users != null && !users.isEmpty()){
            fail("users should be empty");
        }
    }


    @Test
    @DataSet(value = "users.yml", cleanBefore = true, cleanAfter = true)
    public void shouldCleanDatabaseBefore() {
        List<User> users = em().createQuery("select u from User u").getResultList();
        assertThat(users).isNotNull().hasSize(2);//dataset has 2 users, user inserted in @Before must not be present
        User userInsertedInBefore = new User(6);//user inserted in @before has id 6
        assertThat(users).doesNotContain(userInsertedInBefore);
    }
}
