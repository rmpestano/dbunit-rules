package com.github.dbunit.rules;

import com.github.dbunit.rules.api.dataset.DataSet;
import com.github.dbunit.rules.api.dataset.ExpectedDataSet;
import com.github.dbunit.rules.model.User;
import com.github.dbunit.rules.util.EntityManagerProvider;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.github.dbunit.rules.util.EntityManagerProvider.em;
import static com.github.dbunit.rules.util.EntityManagerProvider.tx;

/**
 * Created by rmpestano on 6/15/16.
 */
// tag::expectedDeclaration[]
@RunWith(JUnit4.class)
public class ExpectedDataSetIt {

    @Rule
    public EntityManagerProvider emProvider = EntityManagerProvider.instance("rules-it");

    @Rule
    public DBUnitRule dbUnitRule = DBUnitRule.instance(emProvider.getConnection());


// end::expectedDeclaration[]

    // tag::expected[]
    @Test
    @ExpectedDataSet(value = "yml/expectedUsers.yml",ignoreCols = "id")
    public void shouldMatchExpectedDataSet() {
        User u = new User();
        u.setName("expected user1");
        User u2 = new User();
        u2.setName("expected user2");
        tx().begin();
        em().persist(u);
        em().persist(u2);
        tx().commit();
    }
    // end::expected[]

    @Ignore(value = "How to test failled comparisons?")
    // tag::faillingExpected[]
    @Test
    @ExpectedDataSet(value = "yml/expectedUsers.yml",ignoreCols = "id")
    public void shouldNotMatchExpectedDataSet() {
        User u = new User();
        u.setName("non expected user1");
        User u2 = new User();
        u2.setName("non expected user2");
        tx().begin();
        em().persist(u);
        em().persist(u2);
        tx().commit();
    }
    // end::faillingExpected[]

    // tag::expectedRegex[]
    @Test
    @ExpectedDataSet(value = "yml/expectedUsersRegex.yml")
    public void shouldMatchExpectedDataSetUsingRegex() {
        User u = new User();
        u.setName("expected user1");
        User u2 = new User();
        u2.setName("expected user2");
        tx().begin();
        em().persist(u);
        em().persist(u2);
        tx().commit();
    }
    // end::expectedRegex[]

    // tag::expectedWithSeeding[]
    @Test
    @DataSet(value = "yml/user.yml", disableConstraints = true,cleanAfter = true)
    @ExpectedDataSet(value = "yml/expectedUser.yml", ignoreCols = "id")
    public void shouldMatchExpectedDataSetAfterSeedingDataBase() {
        tx().begin();
        em().remove(em().find(User.class,1L));
        tx().commit();
    }
    // end::expectedWithSeeding[]
}
