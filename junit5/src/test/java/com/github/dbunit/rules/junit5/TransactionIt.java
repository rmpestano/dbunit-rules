package com.github.dbunit.rules.junit5;

import com.github.dbunit.rules.junit5.model.User;
import com.github.dbunit.rules.api.connection.ConnectionHolder;
import com.github.dbunit.rules.api.dataset.DataSet;
import com.github.dbunit.rules.api.dataset.ExpectedDataSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

/**
 * Created by rmpestano on 6/21/16.
 */

@ExtendWith(DBUnitExtension.class)
@RunWith(JUnitPlatform.class)
public class TransactionIt {


    private ConnectionHolder connectionHolder = () ->
            instance("junit5-pu").connection();

    @Test
    @DataSet(cleanBefore = true)
    @ExpectedDataSet(value = "expectedUsersRegex.yml")
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
    @ExpectedDataSet(value = "expectedUsersRegex.yml")
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
