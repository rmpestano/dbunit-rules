package com.github.dbunit.rules.junit5;

import com.github.dbunit.rules.junit5.model.User;
import com.github.dbunit.rules.api.connection.ConnectionHolder;
import com.github.dbunit.rules.api.dataset.DataSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.util.List;

import static com.github.dbunit.rules.util.EntityManagerProvider.em;
import static com.github.dbunit.rules.util.EntityManagerProvider.instance;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by pestano on 07/09/16.
 */
@ExtendWith(DBUnitExtension.class)
@RunWith(JUnitPlatform.class)
@DataSet("users.yml")
public class TestLevelIt {

    private ConnectionHolder connectionHolder = () -> //<3>
            instance("junit5-pu").connection();

    @Test
    public void shouldListUsers() {
        List<User> users = em().createQuery("select u from User u").getResultList();
        assertThat(users).isNotNull().isNotEmpty().hasSize(2);
    }
}
