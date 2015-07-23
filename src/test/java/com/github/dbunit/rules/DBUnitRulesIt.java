package com.github.dbunit.rules;

import com.github.dbunit.rules.dataset.DataSet;
import com.github.dbunit.rules.jpa.EntityManagerProvider;
import com.github.dbunit.rules.model.User;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.extractProperty;

/**
 * Created by pestano on 23/07/15.
 */

@RunWith(JUnit4.class)
public class DBUnitRulesIt {

    @Rule
    public EntityManagerProvider emProvider = EntityManagerProvider.persistenceUnit("rules-it");

    @Rule
    public DBUnitRule dbUnitRule = DBUnitRule.instance(emProvider.getConnection());

    @Test
    @DataSet(value = "datasets/yml/incomplete-user-dataset.yml",disableConstraints = true)
    public void shouldSeedDataSetDisablingContraints() {
        User user = (User) emProvider.em().createQuery("select u from User u where u.id = 1").getSingleResult();
        assertThat(user).isNotNull();
        assertThat(user.getId()).isEqualTo(1);
    }

    @Test
    @DataSet(value = "datasets/yml/incomplete-user-dataset.yml", executeStatementsBefore = "SET DATABASE REFERENTIAL INTEGRITY FALSE;")
    public void shouldSeedDataSetDisablingContraintsViaStatement() {
        User user = (User) emProvider.em().createQuery("select u from User u where u.id = 1").getSingleResult();
        assertThat(user).isNotNull();
        assertThat(user.getId()).isEqualTo(1);
    }

    @Test
    @DataSet(value = "datasets/yml/incomplete-user-dataset.yml", useSequenceFiltering = true)
    public void shouldSeedDataSetUsingSequenceFilter() {
        User user = (User) emProvider.em().createQuery("select u from User u where u.id = 1").getSingleResult();
        assertThat(user).isNotNull();
        assertThat(user.getId()).isEqualTo(1);
    }

}
