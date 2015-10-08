package com.github.dbunit.rules.examples;

import com.github.dbunit.rules.jpa.EntityManagerProvider;
import org.example.CdiConfig;
import org.hibernate.Session;
import org.hibernate.internal.SessionImpl;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.Dependent;
import javax.enterprise.context.RequestScoped;
import javax.enterprise.inject.Produces;
import javax.enterprise.inject.Specializes;
import javax.enterprise.inject.spi.InjectionPoint;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.PersistenceContext;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;


@Specializes
@ApplicationScoped
public class CdiTestConfig extends CdiConfig {

   private EntityManager em;


    @Produces
    public EntityManager produce(){
      synchronized (this){
        return EntityManagerProvider.instance("customerDB").em();
      }
    }

}
