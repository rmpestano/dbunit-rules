package com.github.dbunit.rules.jpa;

/**
 * from https://github.com/AdamBien/rulz/tree/master/em/
 * only difference is is that we need jdbc connection to create dataset
 */

import org.hibernate.Session;
import org.hibernate.internal.SessionImpl;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.*;

public class EntityManagerProvider implements TestRule {

    private EntityManagerFactory emf;
    private EntityManager em;
    private EntityTransaction tx;
    private Connection conn;
    private Map<String,Object> emfProps;
    private static Logger log = LoggerFactory.getLogger(EntityManagerProvider.class);

    private static EntityManagerProvider instance;

    private EntityManagerProvider() {
    }

    public static EntityManagerProvider instance(String unitName){
        if(instance == null){
            instance = new EntityManagerProvider();
        }

        try {
            instance.init(unitName);
        }catch (Exception e){
            log.error("Could not initialize persistence unit " + unitName, e);
        }

        return instance;
    }

    private void init(String unitName) {
        if(emfProps != null){
            emf = Persistence.createEntityManagerFactory(unitName, emfProps);
        }else{
            //first time create database and store emf properties
            emf = Persistence.createEntityManagerFactory(unitName);
            emfProps = new HashMap<>();
            emfProps.putAll(emf.getProperties());
            //avoid database (re)creation
            //FIXME identify current provider and set its property
            emfProps.put("javax.persistence.schema-generation.database.action","none");
            emfProps.put("eclipselink.ddl-generation","none");
            emfProps.put("hibernate.hbm2ddl.auto","validate");
        }
        this.em = emf.createEntityManager();
        this.tx = this.em.getTransaction();
        if(em.getDelegate() instanceof Session){
            conn = ((SessionImpl) em.unwrap(Session.class)).connection();
        } else{
            /**
             * see here:http://wiki.eclipse.org/EclipseLink/Examples/JPA/EMAPI#Getting_a_JDBC_Connection_from_an_EntityManager
             */
            tx.begin();
            conn = em.unwrap(java.sql.Connection.class);
            tx.commit();
        }

    }


    public static Connection getConnection() {
        return instance.conn;
    }

    public static EntityManager em() {
        return instance.em;
    }

    public static EntityTransaction tx() {
        return instance.tx;
    }

    @Override
    public Statement apply(final Statement base, Description description) {
        return new Statement() {

            @Override
            public void evaluate() throws Throwable {
                base.evaluate();
                em.clear();
            }

        };
    }

}