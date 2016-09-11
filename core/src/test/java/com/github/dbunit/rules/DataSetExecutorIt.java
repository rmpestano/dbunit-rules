package com.github.dbunit.rules;

import static com.github.dbunit.rules.cdi.util.EntityManagerProvider.em;
import static com.github.dbunit.rules.cdi.util.EntityManagerProvider.instance;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import com.github.dbunit.rules.cdi.connection.ConnectionHolderImpl;
import com.github.dbunit.rules.cdi.dataset.DataSetExecutorImpl;
import com.github.dbunit.rules.configuration.DataSetConfig;
import com.github.dbunit.rules.cdi.exception.DataBaseSeedingException;
import com.github.dbunit.rules.cdi.util.EntityManagerProvider;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.github.dbunit.rules.model.Follower;
import com.github.dbunit.rules.model.User;

/**
 * Created by pestano on 23/07/15.
 */

@RunWith(JUnit4.class)
public class DataSetExecutorIt {

    public EntityManagerProvider emProvider = instance("executor-it");

    private static DataSetExecutorImpl executor;

    @BeforeClass
    public static void setup() {
        executor = DataSetExecutorImpl.instance("executor-name", new ConnectionHolderImpl(instance("executor-it").connection()));
    }

    @AfterClass
    public static void tearDown() throws SQLException {
        Connection connection = executor.getConnection();
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    @Test
    public void shouldSeedDataSetDisablingContraints() {
        DataSetConfig DataSetConfig = new DataSetConfig("datasets/yml/users.yml").disableConstraints(true);
        executor.createDataSet(DataSetConfig);
        User user = (User) em().createQuery("select u from User u where u.id = 1").getSingleResult();
        assertThat(user).isNotNull();
        assertThat(user.getId()).isEqualTo(1);
    }

    @Test
    public void shouldSeedDataSetDisablingContraintsViaStatement() {
        DataSetConfig DataSetConfig = new DataSetConfig("datasets/yml/users.yml").executeStatementsAfter(new String[]{"SET DATABASE REFERENTIAL INTEGRITY FALSE;"});
        executor.createDataSet(DataSetConfig);
        User user = (User) em().createQuery("select u from User u where u.id = 1").getSingleResult();
        assertThat(user).isNotNull();
        assertThat(user.getId()).isEqualTo(1);
    }


    @Test
    public void shouldNotSeedDataSetWithoutSequenceFilter() {
        DataSetConfig DataSetConfig = new DataSetConfig("datasets/yml/users.yml").
            useSequenceFiltering(false).
            executeStatementsAfter(new String[] { "DELETE FROM User" });//needed because other tests creates users and as the dataset is not created in this test the CLEAN is not performed
        try {
            executor.createDataSet(DataSetConfig);
        }catch (DataBaseSeedingException e){
            assertThat(e.getMessage()).isEqualTo("Could not initialize dataset: datasets/yml/users.yml");
        }
    }

    @Test
    public void shouldSeedDataSetUsingTableCreationOrder() {
        DataSetConfig DataSetConfig = new DataSetConfig("datasets/yml/users.yml").
            tableOrdering(new String[]{"USER","TWEET","FOLLOWER"}).
            executeStatementsBefore(new String[]{"DELETE FROM FOLLOWER","DELETE FROM TWEET","DELETE FROM USER"}).//needed because other tests created user dataset
           useSequenceFiltering(false);
        DataSetExecutorImpl.instance(new ConnectionHolderImpl(emProvider.connection())).createDataSet(DataSetConfig);
        List<User> users =  em().createQuery("select u from User u").getResultList();
        assertThat(users).hasSize(2);
    }

    @Test
    public void shouldSeedUserDataSet() {
        DataSetConfig DataSetConfig = new DataSetConfig("datasets/yml/users.yml").
            useSequenceFiltering(true);
        executor.createDataSet(DataSetConfig);
        User user = (User) em().createQuery("select u from User u where u.id = 1").getSingleResult();
        assertThat(user).isNotNull();
        assertThat(user.getId()).isEqualTo(1);
    }

    @Test
    public void shouldLoadUserFollowers() {
        DataSetConfig DataSetConfig = new DataSetConfig("datasets/yml/users.yml");
        executor.createDataSet(DataSetConfig);
        User user = (User) em().createQuery("select u from User u left join fetch u.followers where u.id = 1").getSingleResult();
        assertThat(user).isNotNull();
        assertThat(user.getId()).isEqualTo(1);
        assertThat(user.getTweets()).hasSize(1);
        Assert.assertEquals(user.getTweets().get(0).getContent(), "dbunit rules!");
        assertThat(user.getFollowers()).isNotNull().hasSize(1);
        Follower expectedFollower = new Follower(2,1);
        assertThat(user.getFollowers()).contains(expectedFollower);
    }

    @Test
    public void shouldLoadUsersFromJsonDataset() {
        DataSetConfig DataSetConfig = new DataSetConfig("datasets/json/users.json");
        executor.createDataSet(DataSetConfig);
        User user = (User) emProvider.clear().em().createQuery("select u from User u left join fetch u.followers where u.id = 1").getSingleResult();
        assertThat(user).isNotNull();
        assertThat(user.getId()).isEqualTo(1);
        assertThat(user.getTweets()).hasSize(1);
        Assert.assertEquals("dbunit rules json example", user.getTweets().get(0).getContent());
        assertThat(user.getFollowers()).isNotNull().hasSize(1);
        Follower expectedFollower = new Follower(2,1);
        assertThat(user.getFollowers()).contains(expectedFollower);
    }

    @Test
    public void shouldLoadUsersFromXmlDataset() {
        DataSetConfig DataSetConfig = new DataSetConfig("datasets/xml/users.xml");
        executor.createDataSet(DataSetConfig);
        User user = (User) emProvider.clear().em().createQuery("select u from User u left join fetch u.followers where u.id = 1").getSingleResult();
        assertThat(user).isNotNull();
        assertThat(user.getId()).isEqualTo(1);
        assertThat(user.getTweets()).hasSize(1);
        Assert.assertEquals("dbunit rules flat xml example", user.getTweets().get(0).getContent());
        assertThat(user.getFollowers()).isNotNull().hasSize(1);
        Follower expectedFollower = new Follower(2,1);
        assertThat(user.getFollowers()).contains(expectedFollower);
    }

}
