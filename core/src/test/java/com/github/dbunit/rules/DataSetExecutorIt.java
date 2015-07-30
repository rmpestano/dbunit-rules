package com.github.dbunit.rules;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import com.github.dbunit.rules.connection.ConnectionHolderImpl;
import com.github.dbunit.rules.dataset.DataSetExecutor;
import com.github.dbunit.rules.dataset.DataSetModel;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.github.dbunit.rules.model.Follower;
import com.github.dbunit.rules.model.User;

/**
 * Created by pestano on 23/07/15.
 */

@RunWith(JUnit4.class)
public class DataSetExecutorIt {

    @Rule
    public EntityManagerProvider emProvider = EntityManagerProvider.instance("executor-it");
    private static DataSetExecutor executor;

    @BeforeClass
    public static void setup(){
        executor = DataSetExecutor.instance(new ConnectionHolderImpl(EntityManagerProvider.instance("executor-it").getConnection()));//executor is a singleton and 'currentInstance()' should be available in all tests
    }

    @AfterClass
    public static void tearDown() throws SQLException {
        Connection connection = executor.getConnection();
        if(connection != null && !connection.isClosed()){
            connection.close();
        }

    }

    @Test
    public void shouldSeedDataSetDisablingContraints() {
        DataSetModel dataSetModel = new DataSetModel("datasets/yml/users.yml").disableConstraints(true);
        executor.execute(dataSetModel);
        User user = (User) emProvider.em().createQuery("select u from User u where u.id = 1").getSingleResult();
        assertThat(user).isNotNull();
        assertThat(user.getId()).isEqualTo(1);
    }

    @Test
    public void shouldSeedDataSetDisablingContraintsViaStatement() {
        DataSetModel dataSetModel = new DataSetModel("datasets/yml/users.yml").executeStatementsAfter(new String[]{"SET DATABASE REFERENTIAL INTEGRITY FALSE;"});
        executor.execute(dataSetModel);
        User user = (User) emProvider.em().createQuery("select u from User u where u.id = 1").getSingleResult();
        assertThat(user).isNotNull();
        assertThat(user.getId()).isEqualTo(1);
    }


    @Test
    public void shouldNotSeedDataSetWithoutSequenceFilter() {
        DataSetModel dataSetModel = new DataSetModel("datasets/yml/users.yml").
            useSequenceFiltering(false).
            executeStatementsAfter(new String[] { "DELETE FROM User" });//needed because other tests creates users and as the dataset is not created in this test the CLEAN is not performed
        executor.execute(dataSetModel);
        List<User> users =  emProvider.em().createQuery("select u from User u").getResultList();
        assertThat(users).isEmpty();
    }

    @Test
    public void shouldSeedDataSetUsingTableCreationOrder() {
        DataSetModel dataSetModel = new DataSetModel("datasets/yml/users.yml").
            tableOrdering(new String[]{"USER","TWEET","FOLLOWER"}).
            executeStatementsBefore(new String[]{"DELETE FROM FOLLOWER","DELETE FROM TWEET","DELETE FROM USER"}).//needed because other tests created user dataset
           useSequenceFiltering(false);
        DataSetExecutor.instance(new ConnectionHolderImpl(emProvider.getConnection())).execute(dataSetModel);
        List<User> users =  emProvider.em().createQuery("select u from User u").getResultList();
        assertThat(users).hasSize(2);
    }

    @Test
    public void shouldSeedUserDataSet() {
        DataSetModel dataSetModel = new DataSetModel("datasets/yml/users.yml").
            useSequenceFiltering(true);
        executor.execute(dataSetModel);
        User user = (User) emProvider.em().createQuery("select u from User u where u.id = 1").getSingleResult();
        assertThat(user).isNotNull();
        assertThat(user.getId()).isEqualTo(1);
    }

    @Test
    public void shouldLoadUserFollowers() {
        DataSetModel dataSetModel = new DataSetModel("datasets/yml/users.yml");
        executor.execute(dataSetModel);
        User user = (User) emProvider.em().createQuery("select u from User u left join fetch u.followers where u.id = 1").getSingleResult();
        assertThat(user).isNotNull();
        assertThat(user.getId()).isEqualTo(1);
        assertThat(user.getTweets()).hasSize(1);
        assertEquals(user.getTweets().get(0).getContent(), "dbunit rules!");
        assertThat(user.getFollowers()).isNotNull().hasSize(1);
        Follower expectedFollower = new Follower(2,1);
        assertThat(user.getFollowers()).contains(expectedFollower);
    }

    @Test
    public void shouldLoadUsersFromJsonDataset() {
        DataSetModel dataSetModel = new DataSetModel("datasets/json/users.json");
        executor.execute(dataSetModel);
        User user = (User) emProvider.em().createQuery("select u from User u left join fetch u.followers where u.id = 1").getSingleResult();
        assertThat(user).isNotNull();
        assertThat(user.getId()).isEqualTo(1);
        assertThat(user.getTweets()).hasSize(1);
        assertEquals("dbunit rules json example",user.getTweets().get(0).getContent());
        assertThat(user.getFollowers()).isNotNull().hasSize(1);
        Follower expectedFollower = new Follower(2,1);
        assertThat(user.getFollowers()).contains(expectedFollower);
    }

    @Test
    public void shouldLoadUsersFromXmlDataset() {
        DataSetModel dataSetModel = new DataSetModel("datasets/xml/users.xml");
        executor.execute(dataSetModel);
        User user = (User) emProvider.em().createQuery("select u from User u left join fetch u.followers where u.id = 1").getSingleResult();
        assertThat(user).isNotNull();
        assertThat(user.getId()).isEqualTo(1);
        assertThat(user.getTweets()).hasSize(1);
        assertEquals("dbunit rules flat xml example",user.getTweets().get(0).getContent());
        assertThat(user.getFollowers()).isNotNull().hasSize(1);
        Follower expectedFollower = new Follower(2,1);
        assertThat(user.getFollowers()).contains(expectedFollower);
    }

}
