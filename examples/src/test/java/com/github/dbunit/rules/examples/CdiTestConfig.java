package com.github.dbunit.rules.examples;

import com.github.dbunit.rules.util.EntityManagerProvider;
import org.example.CdiConfig;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.enterprise.inject.Specializes;
import javax.persistence.EntityManager;


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
