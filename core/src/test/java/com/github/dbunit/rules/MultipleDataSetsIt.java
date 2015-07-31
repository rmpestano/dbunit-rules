package com.github.dbunit.rules;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import java.util.List;

import com.github.dbunit.rules.dataset.DataSetExecutor;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.github.dbunit.rules.dataset.DataSet;
import com.github.dbunit.rules.model.Follower;
import com.github.dbunit.rules.model.User;

/**
 * Created by pestano on 23/07/15.
 */

@RunWith(JUnit4.class)
public class MultipleDataSetsIt {

    @Rule
    public EntityManagerProvider emProvider = EntityManagerProvider.instance("dataset-pu");

    @Rule
    public EntityManagerProvider emProvider1 = EntityManagerProvider.instance("dataset1-pu");

    @Rule
    public EntityManagerProvider emProvider2 = EntityManagerProvider.instance("dataset2-pu");

    @Rule
    public DBUnitRule exec1Rule = DBUnitRule.instance("exec1",emProvider1.getConnection());

    @Rule
    public DBUnitRule exec2Rule = DBUnitRule.instance("exec2",emProvider2.getConnection());

    @Rule
    public DBUnitRule defaultExecutorRule = DBUnitRule.instance(emProvider.getConnection());




    @Test
    @DataSet(value = "datasets/yml/users.yml",disableConstraints = true, executorName = "exec1")
    public void shouldSeedDataSetDisablingContraints() {
        User user = (User) emProvider1.em().createQuery("select u from User u where u.id = 1").getSingleResult();
        assertThat(user).isNotNull();
        assertThat(user.getId()).isEqualTo(1);
    }

    @Test
    @DataSet(value = "datasets/yml/users.yml",disableConstraints = true, executorName = "exec2")
    public void shouldSeedDataSetDisablingContraints2() {
        User user = (User) emProvider2.em().createQuery("select u from User u where u.id = 1").getSingleResult();
        assertThat(user).isNotNull();
        assertThat(user.getId()).isEqualTo(1);
    }

    @Test
    @DataSet(value = "datasets/yml/users.yml", executeStatementsBefore = "SET DATABASE REFERENTIAL INTEGRITY FALSE;", executorName = "exec1")
    public void shouldSeedDataSetDisablingContraintsViaStatement() {
        User user = (User) emProvider1.em().createQuery("select u from User u where u.id = 1").getSingleResult();
        assertThat(user).isNotNull();
        assertThat(user.getId()).isEqualTo(1);
    }

    @Test
    @DataSet(value = "datasets/yml/users.yml", executeStatementsBefore = "SET DATABASE REFERENTIAL INTEGRITY FALSE;", executorName = "exec2")
    public void shouldSeedDataSetDisablingContraintsViaStatement2() {
        User user = (User) emProvider2.em().createQuery("select u from User u where u.id = 1").getSingleResult();
        assertThat(user).isNotNull();
        assertThat(user.getId()).isEqualTo(1);
    }




    @Test
    @DataSet(value = "datasets/yml/users.yml",
            useSequenceFiltering = false,
            executorName = "exec1",
            executeStatementsBefore = "DELETE FROM User"//needed because other tests creates users and as the dataset is not created in this test the CLEAN is not performed
    )
    public void shouldNotSeedDataSetWithoutSequenceFilter() {
        List<User> users =  emProvider1.em().createQuery("select u from User u").getResultList();
        assertThat(users).isEmpty();
    }


    @Test
    @DataSet(value = "datasets/yml/users.yml",
        useSequenceFiltering = false,
        executeStatementsBefore = "DELETE FROM User"//needed because other tests creates users and as the dataset is not created in this test the CLEAN is not performed
    )
    public void shouldNotSeedDataSetWithoutSequenceFilterUsingDefaultExecutor() {
        List<User> users =  emProvider.em().createQuery("select u from User u").getResultList();
        assertThat(users).isEmpty();
    }

    @Test
    @DataSet(value = "datasets/yml/users.yml",
        useSequenceFiltering = false,
        executorName = "exec2",
        executeStatementsBefore = "DELETE FROM User"//needed because other tests creates users and as the dataset is not created in this test the CLEAN is not performed
    )
    public void shouldNotSeedDataSetWithoutSequenceFilter2() {
        List<User> users =  emProvider2.em().createQuery("select u from User u").getResultList();
        assertThat(users).isEmpty();
    }

    @Test
    @DataSet(value = "datasets/yml/users.yml",
        useSequenceFiltering = false,
        executorName = "exec1",
        tableOrdering = {"USER","TWEET","FOLLOWER"},
        executeStatementsBefore = {"DELETE FROM FOLLOWER","DELETE FROM TWEET","DELETE FROM USER"}//needed because other tests created user dataset
     )
     public void shouldSeedDataSetUsingTableCreationOrder() {
        List<User> users =  emProvider1.em().createQuery("select u from User u").getResultList();
        assertThat(users).hasSize(2);
    }

    @Test
    @DataSet(value = "datasets/yml/users.yml",
        useSequenceFiltering = false,
        executorName = "exec2",
        tableOrdering = {"USER","TWEET","FOLLOWER"},
        executeStatementsBefore = {"DELETE FROM FOLLOWER","DELETE FROM TWEET","DELETE FROM USER"}//needed because other tests created user dataset
    )
    public void shouldSeedDataSetUsingTableCreationOrder2() {
        List<User> users =  emProvider2.em().createQuery("select u from User u").getResultList();
        assertThat(users).hasSize(2);
    }

    @Test
    @DataSet(value = "datasets/yml/users.yml", useSequenceFiltering = true,executorName = "exec1")
    public void shouldSeedUserDataSet() {
        User user = (User) emProvider1.em().createQuery("select u from User u where u.id = 1").getSingleResult();
        assertThat(user).isNotNull();
        assertThat(user.getId()).isEqualTo(1);
    }

    @Test
    @DataSet(value = "datasets/yml/users.yml", useSequenceFiltering = true,executorName = "exec2")
    public void shouldSeedUserDataSet2() {
        User user = (User) emProvider2.em().createQuery("select u from User u where u.id = 1").getSingleResult();
        assertThat(user).isNotNull();
        assertThat(user.getId()).isEqualTo(1);
    }

    @Test
    @DataSet(value = "datasets/yml/users.yml")
    public void shouldLoadUserFollowersWithDefaultExecutor() {
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
    @DataSet(value = "datasets/yml/users.yml",executorName = "exec1")
    public void shouldLoadUserFollowers() {
        User user = (User) emProvider1.em().createQuery("select u from User u left join fetch u.followers where u.id = 1").getSingleResult();
        assertThat(user).isNotNull();
        assertThat(user.getId()).isEqualTo(1);
        assertThat(user.getTweets()).hasSize(1);
        assertEquals(user.getTweets().get(0).getContent(), "dbunit rules!");
        assertThat(user.getFollowers()).isNotNull().hasSize(1);
        Follower expectedFollower = new Follower(2,1);
        assertThat(user.getFollowers()).contains(expectedFollower);
    }

    @Test
    @DataSet(value = "datasets/yml/users.yml",executorName = "exec2")
    public void shouldLoadUserFollowers2() {
        User user = (User) emProvider2.em().createQuery("select u from User u left join fetch u.followers where u.id = 1").getSingleResult();
        assertThat(user).isNotNull();
        assertThat(user.getId()).isEqualTo(1);
        assertThat(user.getTweets()).hasSize(1);
        assertEquals(user.getTweets().get(0).getContent(), "dbunit rules!");
        assertThat(user.getFollowers()).isNotNull().hasSize(1);
        Follower expectedFollower = new Follower(2,1);
        assertThat(user.getFollowers()).contains(expectedFollower);
    }



    @Test
    @DataSet(value = "datasets/json/users.json",executorName = "exec1")
    public void shouldLoadUsersFromJsonDataset() {
        User user = (User) emProvider1.em().createQuery("select u from User u left join fetch u.followers where u.id = 1").getSingleResult();
        assertThat(user).isNotNull();
        assertThat(user.getId()).isEqualTo(1);
        assertThat(user.getTweets()).hasSize(1);
        assertEquals("dbunit rules json example",user.getTweets().get(0).getContent());
        assertThat(user.getFollowers()).isNotNull().hasSize(1);
        Follower expectedFollower = new Follower(2,1);
        assertThat(user.getFollowers()).contains(expectedFollower);
    }

    @Test
    @DataSet(value = "datasets/json/users.json",executorName = "exec2")
    public void shouldLoadUsersFromJsonDataset2() {
        User user = (User) emProvider2.em().createQuery("select u from User u left join fetch u.followers where u.id = 1").getSingleResult();
        assertThat(user).isNotNull();
        assertThat(user.getId()).isEqualTo(1);
        assertThat(user.getTweets()).hasSize(1);
        assertEquals("dbunit rules json example",user.getTweets().get(0).getContent());
        assertThat(user.getFollowers()).isNotNull().hasSize(1);
        Follower expectedFollower = new Follower(2,1);
        assertThat(user.getFollowers()).contains(expectedFollower);
    }

    @Test
    @DataSet(value = "datasets/xml/users.xml",executorName = "exec1")
    public void shouldLoadUsersFromXmlDataset() {
        User user = (User) emProvider1.em().createQuery("select u from User u left join fetch u.followers where u.id = 1").getSingleResult();
        assertThat(user).isNotNull();
        assertThat(user.getId()).isEqualTo(1);
        assertThat(user.getTweets()).hasSize(1);
        assertEquals("dbunit rules flat xml example",user.getTweets().get(0).getContent());
        assertThat(user.getFollowers()).isNotNull().hasSize(1);
        Follower expectedFollower = new Follower(2,1);
        assertThat(user.getFollowers()).contains(expectedFollower);
    }

    @Test
    @DataSet(value = "datasets/xml/users.xml",executorName = "exec2")
    public void shouldLoadUsersFromXmlDataset2() {
        User user = (User) emProvider2.em().createQuery("select u from User u left join fetch u.followers where u.id = 1").getSingleResult();
        assertThat(user).isNotNull();
        assertThat(user.getId()).isEqualTo(1);
        assertThat(user.getTweets()).hasSize(1);
        assertEquals("dbunit rules flat xml example",user.getTweets().get(0).getContent());
        assertThat(user.getFollowers()).isNotNull().hasSize(1);
        Follower expectedFollower = new Follower(2,1);
        assertThat(user.getFollowers()).contains(expectedFollower);
    }

}
