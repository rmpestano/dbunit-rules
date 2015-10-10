package com.github.dbunit.rules;

import com.github.dbunit.rules.cdi.api.UsingDataSet;
import com.github.dbunit.rules.model.User;
import junit.framework.Assert;
import org.apache.deltaspike.testcontrol.api.junit.CdiTestRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.inject.Inject;
import javax.persistence.EntityManager;
import javax.persistence.NoResultException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

/**
 * Created by pestano on 09/10/15.
 */
@RunWith(CdiTestRunner.class)
public class BeforeAndAfterTest {

    @Inject
    EntityManager em;

    @Before
    public void init() {
        em.getTransaction().begin();
        em.createNativeQuery("INSERT INTO USER VALUES (6,'user6')").executeUpdate();
        em.flush();
        em.getTransaction().commit();
        assertNotNull(getUser(6));
    }

    @Test
     @UsingDataSet(value = "yml/users.yml", seedStrategy = UsingDataSet.SeedStrategy.INSERT,
             cleanBefore = true, cleanAfter = true
    )
    public void shouldClearDatabaseBeforeAndAfter() {
        try {
            assertNull(getUser(6));
            fail();//should not get here
        }catch (NoResultException nre){
            //no op
        }
        assertNotNull(getUser(1));//inserted by dbunit, see users.yml
    }

    public User getUser(int id) {
        return em.createQuery("select u from User u where u.id = " + id, User.class).getSingleResult();
    }

    @After
    public void end() {//clean after must delete rows inserted by dbunit
        assertTrue(em.createNativeQuery("select * from USER u").getResultList().isEmpty());
    }
}
