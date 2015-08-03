package com.github.dbunit.rules.examples;

import com.github.dbunit.rules.cdi.api.DataSetInterceptor;
import com.github.dbunit.rules.api.dataset.DataSet;
import com.github.dbunit.rules.cdi.api.JPADataSet;
import com.github.dbunit.rules.jpa.JPADataSetExecutor;
import org.apache.deltaspike.testcontrol.api.junit.CdiTestRunner;
import org.example.jpadomain.Company;
import org.example.jpadomain.Contact;
import org.example.service.deltaspike.CompanyRepository;
import org.example.service.deltaspike.DeltaSpikeContactService;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.inject.Inject;
import javax.persistence.EntityManager;
import java.util.Comparator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;

/**
 * Created by pestano on 23/07/15.
 */

@RunWith(CdiTestRunner.class)
@DataSetInterceptor
public class DeltaspikeUsingInterceptorIt {

    static {
        System.setProperty("db","interceptorDB");
    }

    @Inject
    EntityManager entityManager;

    @Inject
    DeltaSpikeContactService contactService;

    @Inject
    CompanyRepository companyRepository;

    @Inject
    @JPADataSet(value = "datasets/contacts.yml",unitName = "interceptorDB")
    JPADataSetExecutor dataSetExecutor;


    @Test
    @JPADataSet(value = "datasets/contacts.yml",unitName = "interceptorDB")
    public void shouldQueryAllCompanies() {
        assertNotNull(contactService);
        assertThat(contactService.findCompanies()).hasSize(4);
    }

    @Test
    @JPADataSet(value = "datasets/contacts.json",unitName = "interceptorDB")
    public void shouldQueryAllContactsUsingJsonDataSet() {
        assertThat(companyRepository.count()).isEqualTo(4);
    }

    @Test
    @JPADataSet(value = "datasets/contacts.yml",unitName = "interceptorDB")
    public void shouldFindCompanyByName() {
        Company expectedCompany = new Company("Google");
        assertNotNull(companyRepository);
        assertThat(companyRepository.findByName("Google")).
                isNotNull().usingElementComparator(new Comparator<Company>() {
            @Override
            public int compare(Company o1, Company o2) {
                return o1.getName().compareTo(o2.getName());
            }
        }).contains(expectedCompany);
    }

    @Test
    @JPADataSet(value = "datasets/contacts.yml",unitName = "interceptorDB")
    public void shouldCreateCompany() {
        assertThat(companyRepository.count()).isEqualTo(4);
        Company company = new Company("test company");
        beginTx();
        Company companyCreated = companyRepository.save(company);
        assertThat(companyCreated.id).isNotNull();
        commitTx();
        assertThat(companyRepository.count()).isEqualTo(5);
    }

    @Test
    public void shouldCreateCompanyUsingInjectedDataSetExecutor() {
        dataSetExecutor.createDataSet();
        assertThat(companyRepository.count()).isEqualTo(4);
        Company company = new Company("test company");
        beginTx();
        Company companyCreated = companyRepository.save(company);
        assertThat(companyCreated.id).isNotNull();
        commitTx();
        assertThat(companyRepository.count()).isEqualTo(5);
    }

    @Test
    @JPADataSet(value = "datasets/contacts.yml",unitName = "interceptorDB")
    public void shouldCreateContact() {
        Company google = companyRepository.findByName("Google").get(0);
        assertThat(contactService.countByCompanyAndName(google, "rmpestano")).isEqualTo(0);
        Contact rmpestano = new Contact("rmpestano", null, "rmpestano@gmail.com", google);
        beginTx();//for now deltaspike @Transactional isn't helping
        contactService.save(rmpestano);
        commitTx();
        assertThat(rmpestano.id).isNotNull();
        assertThat(contactService.countByCompanyAndName(google, "rmpestano")).isEqualTo(1);
    }

    @Test
    @JPADataSet(value = "datasets/contacts.yml",unitName = "interceptorDB")
    public void shouldDeleteContact() {
        Company pivotal = companyRepository.findByName("Pivotal").get(0);
        assertThat(contactService.countByCompanyAndName(pivotal, "Spring")).
                isEqualTo(1);
        Contact spring = contactService.findByCompanyAndName(pivotal, "Spring").get(0);
        beginTx();//for now deltaspike @Transactional isn't helping
        contactService.delete(spring);
        commitTx();
        assertThat(contactService.countByCompanyAndName(pivotal, "Spring")).
                isEqualTo(0);
    }


    private void beginTx() {
        entityManager.getTransaction().begin();
    }

    private void commitTx() {
        entityManager.getTransaction().commit();
    }
}
