package com.github.dbunit.rules;

import com.github.dbunit.rules.api.dataset.DataSet;
import com.github.dbunit.rules.api.dataset.ExpectedDataSet;
import com.github.dbunit.rules.model.User;
import com.github.dbunit.rules.util.EntityManagerProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Created by rmpestano on 6/15/16.
 */
@RunWith(JUnit4.class)
public class ExpectedDataSetIt {

    @Rule
    public EntityManagerProvider emProvider = EntityManagerProvider.instance("rules-it");

    @Rule
    public DBUnitRule dbUnitRule = DBUnitRule.instance(emProvider.getConnection());


    @Test
    @ExpectedDataSet(value = "yml/expectedUsers.yml",ignoreCols = "id")
    public void shouldMatchExpectedDataSet() {
        User u = new User();
        u.setName("expected user1");
        User u2 = new User();
        u2.setName("expected user2");
        emProvider.tx().begin();
        emProvider.em().persist(u);
        emProvider.em().persist(u2);
        emProvider.tx().commit();
     /*
        done by expected dataset
        assertThat(u.getId()).isNotNull();
        assertThat(u.getName()).isEqualTo("expected user1");
        assertThat(u2.getId()).isNotNull();
        assertThat(u2.getName()).isEqualTo("expected user2");*/
    }

    @Test
    @ExpectedDataSet(value = "yml/expectedUsersRegex.yml")
    public void shouldMatchExpectedDataSetUsingRegex() {
        User u = new User();
        u.setName("expected user1");
        User u2 = new User();
        u2.setName("expected user2");
        emProvider.tx().begin();
        emProvider.em().persist(u);
        emProvider.em().persist(u2);
        emProvider.tx().commit();
    }

    @Test
    @DataSet(value = "yml/user.yml", disableConstraints = true,cleanAfter = true)
    @ExpectedDataSet(value = "yml/expectedUser.yml", ignoreCols = "id")
    public void shouldMatchExpectedDataSetAfterSeedingDataBase() {
        emProvider.tx().begin();
        emProvider.em().remove(emProvider.em().find(User.class,1L));
        emProvider.tx().commit();
    }
}
