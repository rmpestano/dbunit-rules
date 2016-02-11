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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;
import java.sql.Connection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class EntityManagerProvider implements TestRule {

    private Map<String, EntityManagerFactory> emfs = new ConcurrentHashMap<>();//one emf per unit

    private EntityManager       em;

    private EntityTransaction   tx;

    private Connection          conn;

    private static Logger log = LoggerFactory.getLogger(EntityManagerProvider.class);

    private static EntityManagerProvider instance;

    private EntityManagerProvider() {
    }

    public static synchronized EntityManagerProvider instance(String unitName) {
        if (instance == null) {
            instance = new EntityManagerProvider();
        }

        try {
            instance.init(unitName);
        } catch (Exception e) {
            log.error("Could not initialize persistence unit " + unitName, e);
        }

        return instance;
    }

    private void init(String unitName) {
        EntityManagerFactory emf = emfs.get(unitName);
        if (emf == null) {
            log.debug("creating emf for unit "+unitName);
            emf = Persistence.createEntityManagerFactory(unitName);
            em = emf.createEntityManager();
            this.tx = this.em.getTransaction();
            if (isHibernatePresentOnClasspath() && em.getDelegate() instanceof Session) {
                conn = ((SessionImpl) em.unwrap(Session.class)).connection();
            } else{
                /**
                 * see here:http://wiki.eclipse.org/EclipseLink/Examples/JPA/EMAPI#Getting_a_JDBC_Connection_from_an_EntityManager
                 */
                tx.begin();
                conn = em.unwrap(Connection.class);
                tx.commit();
            }
            emfs.put(unitName,emf);

        }
        emf.getCache().evictAll();

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

    private boolean isHibernatePresentOnClasspath(){
        try {
            Class.forName( "org.hibernate.Session" );
            return true;
        } catch( ClassNotFoundException e ) {
            return false;
        }
    }

}