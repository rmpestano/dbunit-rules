package com.github.dbunit.junit5;

import com.github.dbunit.junit5.model.User;
import com.github.dbunit.rules.api.connection.ConnectionHolder;
import com.github.dbunit.rules.api.dataset.DataSet;
import com.github.dbunit.rules.api.dataset.ExpectedDataSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.util.List;

import static com.github.dbunit.rules.util.EntityManagerProvider.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by pestano on 28/08/16.
 */
@ExtendWith(DBUnitExtension.class)
@RunWith(JUnitPlatform.class)
public class DBUnitJUnit5Test {

    private ConnectionHolder connectionHolder = () -> instance("junit5-pu").connection();

    @Test
    @DataSet("users.yml")
    public void shouldListUsers() {
        List<User> users = em().createQuery("select u from User u").getResultList();
        assertThat(users).isNotNull().isNotEmpty().hasSize(2);
    }

    @Test
    @DataSet(cleanBefore=true) //avoid conflict with other tests 
    public void shouldInsertUser() {
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
    @DataSet("users.yml") //no need for clean before because DBUnit uses CLEAN_INSERT seeding strategy which clears involved tables before seeding
    public void shouldUpdateUser() {
        User user = (User) em().createQuery("select u from User u  where u.id = 1").getSingleResult();
        assertThat(user).isNotNull();
        assertThat(user.getName()).isEqualTo("@realpestano");
        //tx().begin(); 
        user.setName("@rmpestano");
        em().merge(user);
        //tx().commit(); //no needed because of first level cache 
        User updatedUser = getUser(1L);
        assertThat(updatedUser).isNotNull();
        assertThat(updatedUser.getName()).isEqualTo("@rmpestano");
    }

    @Test
    @DataSet(value = "users.yml", disableConstraints=true, transactional = true)//disable constraints because User 1 has one tweet and a follower
    @ExpectedDataSet("expectedUser.yml")
    public void shouldDeleteUser() {
        User user = (User) em().createQuery("select u from User u  where u.id = 1").getSingleResult();
        assertThat(user).isNotNull();
        assertThat(user.getName()).isEqualTo("@realpestano");
        em().remove(user);
    }

    @DataSet("users.yml")
    public void shouldDeleteUserWithoutDisablingConstraints() {
        User user = (User) em().createQuery("select u from User u  where u.id = 1").getSingleResult();
        assertThat(user).isNotNull();
        assertThat(user.getName()).isEqualTo("@realpestano");
        em().getTransaction().begin();
        em().createQuery("Delete from Tweet t where t.user.id = 1 ").executeUpdate();
        em().createQuery("Delete from Follower f where f.followedUser.id = 1 ").executeUpdate();
        em().remove(user);
        em().getTransaction().commit();
        List<User> users = em().createQuery("select u from User u ").getResultList();
        assertThat(users).hasSize(1);
    }


    public User getUser(Long id){
        return (User) em().createQuery("select u from User u where u.id = :id").
                setParameter("id", id).getSingleResult();
    }
}
