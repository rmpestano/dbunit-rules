package com.github.dbunit.rules;

import static com.github.dbunit.rules.util.EntityManagerProvider.em;
import static com.github.dbunit.rules.util.EntityManagerProvider.tx;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.github.dbunit.rules.api.dataset.DataSet;
import com.github.dbunit.rules.api.dataset.ExpectedDataSet;
import com.github.dbunit.rules.model.User;
import com.github.dbunit.rules.util.EntityManagerProvider;

/**
 * Created by rmpestano on 8/21/16.
 */
@RunWith(JUnit4.class)
public class EmptyDataSetIt {

    EntityManagerProvider emProvider = EntityManagerProvider.instance("rules-it");

    @Rule
    public TestRule theRule = RuleChain.outerRule(emProvider).
            around(DBUnitRule.instance(emProvider.connection()));


    @BeforeClass
    public static void init(){
        User user = new User();
        user.setName("user");
        user.setName("@rmpestano");
        tx("rules-it").begin();
        em("rules-it").persist(user);
        tx("rules-it").commit();
    }

    @Test
    @DataSet(value = "yml/empty.yml")
    public void shouldSeedDatabaseWithEmptyDataSet() {
        long count = (long) em().createQuery("select count(u.id) from User u").getSingleResult();
        assertThat(0L).isEqualTo(count);
        User user = new User();
        user.setName("user");
        user.setName("@rmpestano");
        tx().begin();
        em().persist(user);
        tx().commit();
        User insertedUser = (User)em().createQuery("select u from User u where u.name = '@rmpestano'").getSingleResult();
        assertThat(insertedUser).isNotNull();
        assertThat(insertedUser.getId()).isNotNull();
    }

    @Test
    @DataSet("yml/empty.yml")
    @ExpectedDataSet("yml/empty.yml")
    public void shouldSeedAndExpectEmptyDataSet() {
    }

    @Test
    @DataSet(value = "yml/user.yml", disableConstraints = true)
    @ExpectedDataSet(value = "yml/empty.yml")
    public void shouldMatchEmptyYmlDataSet() {
        tx().begin();
        em().remove(em().find(User.class,1L));
        em().remove(em().find(User.class,2L));
        tx().commit();
    }

    @Test
    @DataSet(value = "yml/user.yml", disableConstraints = true, transactional = true)
    @ExpectedDataSet(value = "yml/empty.yml")
    public void shouldMatchEmptyYmlDataSetWithTransaction() {
        em().remove(em().find(User.class,1L));
        em().remove(em().find(User.class,2L));
    }


    @Test
    @DataSet(value = "json/user.json", disableConstraints = true)
    @ExpectedDataSet(value = "json/empty.json")
    public void shouldMatchEmptyJsonDataSet() {
        tx().begin();
        em().remove(em().find(User.class,1L));
        em().remove(em().find(User.class,2L));
        tx().commit();
        em().createQuery("select u from User u").getResultList();
    }

    @Test
    @DataSet(value = "xml/user.xml", disableConstraints = true)
    @ExpectedDataSet(value = "xml/empty.xml")
    public void shouldMatchEmptyXmlDataSet() {
        tx().begin();
        em().remove(em().find(User.class,1L));
        em().remove(em().find(User.class,2L));
        tx().commit();
    }
}
