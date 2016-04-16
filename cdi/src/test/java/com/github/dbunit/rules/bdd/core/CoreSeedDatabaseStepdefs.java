package com.github.dbunit.rules.bdd.core;

import com.github.dbunit.rules.model.User;
import cucumber.api.PendingException;
import cucumber.api.java.en.Given;

import javax.inject.Inject;
import javax.persistence.EntityManager;
import java.util.List;

/**
 * Created by rafael-pestano on 09/10/2015.
 */
public class CoreSeedDatabaseStepdefs {

    @Inject
    EntityManager em;

    List<User> usersFound;



    @Given("^The following junit rules$")
    public void the_following_junit_rules(String docString) throws Throwable {

    }
}
