package com.github.dbunit.rules.jpa;

/**
 * from https://github.com/AdamBien/rulz/tree/master/em/
 * only difference is is that we need jdbc connection to create dataset
 */

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;
import java.sql.Connection;

public class EntityManagerProvider implements TestRule {

    private EntityManager em;
    private EntityTransaction tx;
    private Connection conn;

    private static EntityManagerProvider instance;

    public static EntityManagerProvider instance(String unitName){
        if(instance == null){
            instance = new EntityManagerProvider(unitName);
        }
        return instance;
    }

    private EntityManagerProvider(String unitName) {
        this.em = Persistence.createEntityManagerFactory(unitName).createEntityManager();
        this.tx = this.em.getTransaction();
    }



    /**
     * see here:http://wiki.eclipse.org/EclipseLink/Examples/JPA/EMAPI#Getting_a_JDBC_Connection_from_an_EntityManager
     */
    public Connection getConnection() {
        tx.begin();
        conn = em.unwrap(java.sql.Connection.class);
        tx.commit();
        return conn;
    }

    public EntityManager em() {
        return this.em;
    }

    public EntityTransaction tx() {
        return this.tx;
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