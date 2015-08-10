package com.github.dbunit.rules;

import com.github.dbunit.rules.api.dataset.DataSetModel;
import com.github.dbunit.rules.jpa.EntityManagerProvider;
import com.github.dbunit.rules.jpa.JPADataSetExecutor;
import com.github.dbunit.rules.model.User;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

/**
 * Created by pestano on 23/07/15.
 */

@RunWith(JUnit4.class)
public class JPADatasetExecutorIt {

    @Rule
    public EntityManagerProvider emProvider = EntityManagerProvider.instance("rules-it");


    @Test
    public void shouldSeedUserDataSetUsingJpaExecutor() {
        DataSetModel dataModel = new DataSetModel("datasets/yml/users.yml");
        JPADataSetExecutor.instance(emProvider.em()).createDataSet(dataModel);
        User user = (User) emProvider.em().createQuery("select u from User u where u.id = 1").getSingleResult();
        assertThat(user).isNotNull();
        assertThat(user.getId()).isEqualTo(1);
    }

    @Test
    public void shouldSeedUserDataSetUsingJpaExecutorWithId() {
        DataSetModel dataModel = new DataSetModel("datasets/yml/users.yml");
        JPADataSetExecutor.instance("executorId",emProvider.em()).createDataSet(dataModel);
        User user = (User) emProvider.em().createQuery("select u from User u join fetch u.tweets join fetch u.followers where u.id = 1").getSingleResult();
        assertThat(user).isNotNull();
        assertThat(user.getId()).isEqualTo(1);
        assertThat(user.getTweets()).hasSize(1);
        assertThat(user.getFollowers()).hasSize(1);
    }



}