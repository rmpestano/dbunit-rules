package com.github.dbunit.rules;

import com.github.dbunit.rules.cdi.DBUnitRule;
import com.github.dbunit.rules.cdi.api.dataset.DataSet;
import com.github.dbunit.rules.cdi.api.dataset.ExpectedDataSet;
import com.github.dbunit.rules.cdi.api.configuration.DBUnit;
import com.github.dbunit.rules.model.User;
import com.github.dbunit.rules.cdi.util.EntityManagerProvider;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.github.dbunit.rules.cdi.util.EntityManagerProvider.em;
import static com.github.dbunit.rules.cdi.util.EntityManagerProvider.tx;

/**
 * Created by rmpestano on 6/21/16.
 */

@RunWith(JUnit4.class)
@DBUnit(cacheConnection = false)
public class TransactionIt {

    @Rule
    public EntityManagerProvider emProvider = EntityManagerProvider.instance("rules-it");

    @Rule
    public DBUnitRule dbUnitRule = DBUnitRule.instance("TransactionIt",emProvider.connection());

    @Test
    @DataSet(cleanBefore = true,executorId = "TransactionIt")
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
    @DataSet(cleanBefore = true, transactional = true,executorId = "TransactionIt")
    @ExpectedDataSet(value = "yml/expectedUsersRegex.yml")
    @DBUnit(cacheConnection = true)
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
