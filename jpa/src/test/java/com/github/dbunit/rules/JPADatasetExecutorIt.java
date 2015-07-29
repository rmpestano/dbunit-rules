package com.github.dbunit.rules;

import com.github.dbunit.rules.dataset.DataSet;
import com.github.dbunit.rules.dataset.DataSetModel;
import com.github.dbunit.rules.jpa.EntityManagerProvider;
import com.github.dbunit.rules.jpa.JPADataSetExecutor;
import com.github.dbunit.rules.model.Follower;
import com.github.dbunit.rules.model.User;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

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
    public void shouldSeedUserDataSetUsing() {
        DataSetModel dataModel = new DataSetModel("datasets/yml/users.yml");
        JPADataSetExecutor.instance(emProvider.em()).execute(dataModel);
        User user = (User) emProvider.em().createQuery("select u from User u where u.id = 1").getSingleResult();
        assertThat(user).isNotNull();
        assertThat(user.getId()).isEqualTo(1);
    }

}