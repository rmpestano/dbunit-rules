package com.github.dbunit.rules.bdd;

import com.github.dbunit.rules.api.dataset.ExpectedDataSet;
import com.github.dbunit.rules.cdi.api.UsingDataSet;
import com.github.dbunit.rules.model.User;
import cucumber.api.PendingException;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;

import javax.inject.Inject;
import javax.persistence.EntityManager;

import static org.junit.Assert.assertNotNull;

/**
 * Created by rafael-pestano on 16/06/2016.
 */
public class GeneralSteps {

    @Inject
    EntityManager em;


    @Given("^Groovy script engine is on test classpath$")
    public void Groovy_script_engine_is_on_test_classpath(String docstring) throws Throwable {
        // Express the Regexp above with the code you wish you had
        assertNotNull(docstring);
    }

    @Then("^Dataset script should be interpreted while seeding the database$")
    public void Dataset_script_should_be_interpreted_when_seeding_the_database() throws Throwable {
        // Express the Regexp above with the code you wish you had
    }

    @Then("^Test must pass because database state is as in expected dataset.$")
    public void Test_must_pass_because_dataBase_state_is_as_expected_in_dataset() throws Throwable {
        // Express the Regexp above with the code you wish you had
    }

    /*@Then("^Test must fail with following error:$")
    @ExpectedDataSet(value = "yml/expectedUsers.yml",ignoreCols = "id")
    @UsingDataSet//needed to activate dbunit cdi interceptor
    public void Test_must_fail_showing_what_it_was_expecting_as_database_state(String docstring) throws Throwable {
        User u = new User();
        u.setName("non expected user1");
        User u2 = new User();
        u2.setName("non expected user2");
        em.getTransaction().begin();
        em.persist(u);
        em.persist(u2);
        em.getTransaction().commit();
    }*/

    @Then("^Test must fail with following error:$")
    public void Test_must_fail_with_following_error(String docstring) throws Throwable {
        assertNotNull(docstring);
    }
}
