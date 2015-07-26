package com.github.dbunit.rules.examples;

import org.example.CdiConfig;
import org.hibernate.Session;
import org.hibernate.internal.SessionImpl;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.Dependent;
import javax.enterprise.context.RequestScoped;
import javax.enterprise.inject.Produces;
import javax.enterprise.inject.Specializes;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.PersistenceContext;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Matti Tahvonen
 */
@Specializes
@ApplicationScoped
public class CdiTestConfig extends CdiConfig {

   private EntityManagerFactory emf;
   private EntityManager em;


    @Produces
    public EntityManager produce(){
        if(emf == null){
            emf = Persistence.createEntityManagerFactory("customerTestDb");
        }
        if(em == null || !em.isOpen()){
            em = emf.createEntityManager();
        }
        return em;
    }

}
