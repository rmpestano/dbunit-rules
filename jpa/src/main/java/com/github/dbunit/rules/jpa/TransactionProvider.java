package com.github.dbunit.rules.jpa;

/**
 * from https://github.com/AdamBien/rulz/tree/master/em/
 * only difference is is that we need jdbc connection to create dataset
 */

import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;


public class TransactionProvider implements MethodRule {


    private EntityManager em;
    private static Logger log = LoggerFactory.getLogger(TransactionProvider.class);

    private static TransactionProvider instance;

    private TransactionProvider() {
    }

    public static TransactionProvider instance(EntityManager em){
        if(instance == null){
            instance = new TransactionProvider();
        }
        try {
            instance.em = em;
        }catch (Exception e){
            log.error("Could not initialize transaction provider",e);
        }

        return instance;
    }


    @Override
    public Statement apply(final Statement statement, final FrameworkMethod frameworkMethod, Object o) {
        if (frameworkMethod.getAnnotation(Transactional.class) != null) {
            if(em == null){
                log.warn(frameworkMethod.getName() + "() - Could not start transaction because entity manager is null");
            } else {
                return new Statement() {

                    @Override
                    public void evaluate() throws Throwable {

                        try {
                            if (em.isOpen()) {
                                em.getTransaction().begin();
                                statement.evaluate();
                                em.getTransaction().commit();
                            } else {
                                log.warn(frameworkMethod.getName() + "() - Could not start transaction because entity manager is closed");
                            }

                        } catch (Exception e) {
                            log.error("Problem executing " + frameworkMethod.getName() + "()", e);
                            em.getTransaction().rollback();
                        }

                    }

                };
            }
        }
        return null;
    }
}