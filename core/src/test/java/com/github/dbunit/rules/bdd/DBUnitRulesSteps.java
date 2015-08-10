package com.github.dbunit.rules.bdd;

import com.github.dbunit.rules.DBUnitRule;
import com.github.dbunit.rules.EntityManagerProvider;
import com.github.dbunit.rules.api.dataset.DataSet;
import com.github.dbunit.rules.api.dataset.DataSetExecutor;
import com.github.dbunit.rules.api.dataset.DataSetModel;
import com.github.dbunit.rules.connection.ConnectionHolderImpl;
import com.github.dbunit.rules.dataset.DataSetExecutorImpl;
import com.github.dbunit.rules.model.User;
import cucumber.api.PendingException;
import cucumber.api.java.Before;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Rule;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

/**
 * Created by pestano on 09/06/15.
 */
public class DBUnitRulesSteps {


	public EntityManagerProvider emProvider = EntityManagerProvider.instance("rules-it");

	/*@Rule not supported https://github.com/cucumber/cucumber-jvm/issues/393
	public DBUnitRule dbUnitRule = DBUnitRule.instance(emProvider.getConnection());*/

	DataSetExecutor executor = DataSetExecutorImpl.instance(new ConnectionHolderImpl(emProvider.getConnection()));

	private List<User> users;


	@Given("^The database is seeded with users$")
	//@DataSet(value = "datasets/yml/users.yml")
	public void The_database_is_seeded_with_users() {
		executor.createDataSet(new DataSetModel("datasets/yml/users.yml"));
	}

	@When("^I list users$")
	public void I_list_users() throws Throwable {
		 users =  emProvider.em().createQuery("select u from User u left join fetch u.tweets left join fetch u.followers").getResultList();
	}

	@Then("^(\\d+) users must be found$")
	public void users_must_be_found(int size) throws Throwable {
		 assertThat(users).isNotNull().hasSize(size);
	}
}
