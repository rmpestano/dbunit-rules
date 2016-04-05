package com.github.dbunit.rules;

import com.github.dbunit.rules.api.dataset.SeedStrategy;
import com.github.dbunit.rules.cdi.api.UsingDataSet;
import com.github.dbunit.rules.model.User;
import org.apache.deltaspike.testcontrol.api.junit.CdiTestRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.yaml.snakeyaml.error.YAMLException;

import javax.inject.Inject;
import javax.persistence.EntityManager;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by pestano on 23/07/15.
 */

@RunWith(CdiTestRunner.class)
public class DBUnitCDITest {

    @Inject
    EntityManager em;


    @Test
    @UsingDataSet(value = "",cleanBefore = true)
    public void shouldFailToSeedEmptyDataSet() {
        List<User> users = (List<User>) em.createQuery("select u from User u").getResultList();
        assertThat(users).isNotNull().hasSize(0);
    }

    @Test
    @UsingDataSet(value="ymlzzz/users.yml",cleanBefore = true)
    public void shouldFailToSeedInexistentYMLDataSet() {
        List<User> users = (List<User>) em.createQuery("select u from User u").getResultList();
        assertThat(users).isNotNull().hasSize(0);
    }

    @Test
    @UsingDataSet(value = "jsonzzz/users.json",cleanBefore = true)
    public void shouldFailToSeedInexistentJSONDataSet() {
        List<User> users = (List<User>) em.createQuery("select u from User u").getResultList();
        assertThat(users).isNotNull().hasSize(0);
    }

    @Test
    @UsingDataSet(value="zzz/users.xml",cleanBefore = true)
    public void shouldFailToSeedInexistentXMLDataSet() {
        List<User> users = (List<User>) em.createQuery("select u from User u").getResultList();
        assertThat(users).isNotNull().hasSize(0);
    }

    @Test
    @UsingDataSet(value="users",cleanBefore = true)
    public void shouldFailToSeedDataSetWithoutExtension() {
        List<User> users = (List<User>) em.createQuery("select u from User u").getResultList();
        assertThat(users).isNotNull().hasSize(0);
    }

    @Test
    @UsingDataSet(value = "users.doc",cleanBefore = true)
    public void shouldFailToSeedUnknownDataSetFormat() {
        List<User> users = (List<User>) em.createQuery("select u from User u").getResultList();
        assertThat(users).isNotNull().hasSize(0);
    }

    @Test
    @UsingDataSet("yml/users.yml")
    public void shouldSeedUserDataSetUsingCdiInterceptor() {
        User user = (User) em.createQuery("select u from User u where u.id = 1").getSingleResult();
        assertThat(user).isNotNull();
        assertThat(user.getId()).isEqualTo(1);
    }

    @Test
    @UsingDataSet("json/users.json")
    public void shouldSeedUserDataSetUsingCdiInterceptorUsingJsonDataSet() {
        User user = (User) em.createQuery("select u from User u join fetch u.tweets join fetch u.followers where u.id = 1").getSingleResult();
        assertThat(user).isNotNull();
        assertThat(user.getId()).isEqualTo(1);
        assertThat(user.getTweets()).hasSize(1);
        assertThat(user.getFollowers()).hasSize(1);
    }

    @Test
    @UsingDataSet("xml/users.xml")
    public void shouldSeedUserDataSetUsingCdiInterceptorUsingXmlDataSet() {
        User user = (User) em.createQuery("select u from User u join fetch u.tweets join fetch u.followers where u.id = 1").getSingleResult();
        assertThat(user).isNotNull();
        assertThat(user.getId()).isEqualTo(1);
        assertThat(user.getTweets()).hasSize(1);
        assertThat(user.getFollowers()).hasSize(1);
    }

    @Test
    @UsingDataSet(value = "yml/users.yml", seedStrategy = SeedStrategy.INSERT, cleanBefore = true,
            executeCommandsBefore = "INSERT INTO USER VALUES (3,'user3')"
    )
    public void shouldExecuteCommandBefore() {
        User user = (User) em.createQuery("select u from User u where u.id = 3").getSingleResult();
        assertThat(user).isNotNull();
        assertThat(user.getId()).isEqualTo(3);
        assertThat(user.getName()).isEqualTo("user3");
    }


    //TODO replacer test
    //TODO execute scripts after test
    //TODO disable constraints

}