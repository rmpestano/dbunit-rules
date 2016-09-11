package com.github.dbunit.rules.examples.cucumber.withoutcdi;

import com.github.dbunit.rules.api.dataset.DataSetExecutor;
import com.github.dbunit.rules.configuration.DataSetConfig;
import com.github.dbunit.rules.connection.ConnectionHolderImpl;
import com.github.dbunit.rules.dataset.DataSetExecutorImpl;
import com.github.dbunit.rules.util.EntityManagerProvider;
import cucumber.api.java.Before;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.example.jpadomain.Contact;

import javax.persistence.Query;

import static com.github.dbunit.rules.util.EntityManagerProvider.em;
import static com.github.dbunit.rules.util.EntityManagerProvider.tx;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ContactStepsWithoutCDI {


    EntityManagerProvider entityManagerProvider = EntityManagerProvider.newInstance("customerTestDB");

    DataSetExecutor dbunitExecutor;

    Long count;


    @Before
    public void setUp() {
        dbunitExecutor = DataSetExecutorImpl.instance(new ConnectionHolderImpl(entityManagerProvider.connection()));
        em().clear();//important to clear JPA first level cache between scenarios
    }


    @Given("^we have a list of contacts2$")
    public void given() {
        dbunitExecutor.createDataSet(new DataSetConfig("contacts.yml"));
        assertEquals(em().createQuery("select count(c.id) from Contact c").getSingleResult(), new Long(3));
    }

    @When("^^we search contacts by name \"([^\"]*)\"2$")
    public void we_search_contacts_by_name_(String name) throws Throwable {
        Contact contact = new Contact();
        contact.setName(name);
        Query query = em().createQuery("select count(c.id) from Contact c where UPPER(c.name) like :name");
        query.setParameter("name", "%" + name.toUpperCase() + "%");
        count = (Long) query.getSingleResult();
    }


    @Then("^we should find (\\d+) contacts2$")
    public void we_should_find_result_contacts(Long result) throws Throwable {
        assertEquals(result, count);
    }


    @When("^we delete contact by id (\\d+) 2$")
    public void we_delete_contact_by_id(long id) throws Throwable {
        tx().begin();
        em().remove(em().find(Contact.class, id));
        tx().commit();
    }

    @Then("^we should not find contact (\\d+) 2$")
    public void we_should_not_find_contacts_in_database(long id) throws Throwable {
        assertNull(em().find(Contact.class, id));
    }
}
