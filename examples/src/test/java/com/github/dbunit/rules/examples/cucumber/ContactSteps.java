package com.github.dbunit.rules.examples.cucumber;

import com.github.dbunit.rules.api.dataset.DataSet;
import com.github.dbunit.rules.cdi.api.UsingDataSet;
import cucumber.api.PendingException;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.apache.deltaspike.data.impl.criteria.selection.SingularAttributeSelection;
import org.example.jpadomain.Contact;
import org.example.jpadomain.Contact_;
import org.example.service.deltaspike.ContactRepository;

import javax.inject.Inject;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by pestano on 12/02/16.
 */
public class ContactSteps {

    @Inject
    ContactRepository contactRepository;

    Long count;

    @Given("^we have contacts in the database$")
    @UsingDataSet("datasets/contacts.yml")
    public void given() {
        assertEquals(contactRepository.count(), new Long(3));
    }


    @When("^^we search contacts by name \"([^\"]*)\"$")
    public void we_search_contacts_by_name_(String name) throws Throwable {
        Contact contact = new Contact();
        contact.setName(name);
        count = contactRepository.countLike(contact, Contact_.name);
    }

    @Then("^we should find (\\d+) contacts$")
    public void we_should_find_result_contacts(Long result) throws Throwable {
        assertEquals(result,count);
    }

    @When("^we delete contact by id (\\d+)$")
    public void we_delete_contact_by_id(long id) throws Throwable {
        contactRepository.remove(contactRepository.findBy(id));
    }

    @Then("^we should not find contacts (\\d+) in database$")
    public void we_should_not_find_contacts_in_database(long id) throws Throwable {
        assertNull(contactRepository.findBy(id));
    }
}
