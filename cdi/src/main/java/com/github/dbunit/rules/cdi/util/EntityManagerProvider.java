package com.github.dbunit.rules.cdi.util;

/**
 * COPIED from JPA module because of maven cyclic dependencies (even with test scope)
 */
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;

public class EntityManagerProvider  {

    private EntityManagerFactory emf;

    private EntityManager em;

    private static EntityManagerProvider instance;

    private static Logger log = LoggerFactory.getLogger(EntityManagerProvider.class);

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
        if (emf == null) {
            log.debug("creating emf for unit "+unitName);
            emf = Persistence.createEntityManagerFactory(unitName);
            em = emf.createEntityManager();
        }
        em.clear();
        emf.getCache().evictAll();
    }



    public EntityManager em() {
        return em;
    }

    public static EntityTransaction tx() {
        return instance.em.getTransaction();
    }


}