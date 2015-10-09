package com.github.dbunit.rules.bdd;

import com.github.dbunit.rules.cdi.api.UsingDataSet;
import com.github.dbunit.rules.model.User;
import cucumber.api.PendingException;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;

import javax.inject.Inject;
import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by rafael-pestano on 09/10/2015.
 */
public class UserStepdefs {

  @Inject
  EntityManager em;

  List<User> usersFound;


  @When("^When I search users who tweeted about \"([^\"]*)\"$")
  @UsingDataSet("yml/users.yml")
  public void When_I_search_users_who_tweeted_about(String tweet) throws Throwable {

    Query q = em.createQuery("select u from User u, Tweet t where t.user.id = u.id and t.content like :tweet ");
    q.setParameter("tweet","%"+tweet+"%");

    usersFound = q.getResultList();
    assertNotNull(usersFound);

  }

  @Then("^I must find (\\d+) of users$")
  public void I_must_find_number_of_users(int usersExpected) throws Throwable {
     assertEquals(usersExpected,usersFound.size());
  }

}
