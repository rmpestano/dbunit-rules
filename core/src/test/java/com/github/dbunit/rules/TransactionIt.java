package com.github.dbunit.rules;

import com.github.dbunit.rules.api.dataset.DataSet;
import com.github.dbunit.rules.api.dataset.ExpectedDataSet;
import com.github.dbunit.rules.api.dbunit.DBUnitConfig;
import com.github.dbunit.rules.model.User;
import com.github.dbunit.rules.util.EntityManagerProvider;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.github.dbunit.rules.util.EntityManagerProvider.em;
import static com.github.dbunit.rules.util.EntityManagerProvider.tx;

/**
 * Created by rmpestano on 6/21/16.
 */

@RunWith(JUnit4.class)
@DBUnitConfig(cacheConnection = true)
public class TransactionIt {

    @Rule
    public EntityManagerProvider emProvider = EntityManagerProvider.instance("rules-it");

    @Rule
    public DBUnitRule dbUnitRule = DBUnitRule.instance(TransactionIt.class.getName(),emProvider.connection());

    @Test
    @DataSet(cleanBefore = true)
    @ExpectedDataSet(value = "yml/expectedUsersRegex.yml")
    public void shouldManageTransactionInsideTest() {
        User u = new User();
        u.setName("expected user1");
        User u2 = new User();
        u2.setName("expected user2");
        tx().begin();
        em().persist(u);
        em().persist(u2);
        tx().commit();
    }

    //tag::transaction[]
    @Test
    @DataSet(cleanBefore = true, transactional = true)
    @ExpectedDataSet(value = "yml/expectedUsersRegex.yml")
    public void shouldManageTransactionAutomatically() {
        User u = new User();
        u.setName("expected user1");
        User u2 = new User();
        u2.setName("expected user2");
        em().persist(u);
        em().persist(u2);
    }
    //end::transaction[]


}
