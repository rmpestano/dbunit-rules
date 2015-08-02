package com.github.dbunit.rules.cdi;

import com.github.dbunit.rules.api.dataset.DataSetExecutor;
import com.github.dbunit.rules.api.dataset.DataSetModel;
import com.github.dbunit.rules.cdi.api.JPADataSet;
import com.github.dbunit.rules.jpa.EntityManagerProvider;
import com.github.dbunit.rules.jpa.JPADataSetExecutor;

import javax.enterprise.inject.Produces;
import javax.enterprise.inject.spi.InjectionPoint;
import javax.persistence.EntityManager;
import java.util.UUID;

/**
 * Created by pestano on 01/08/15.
 */
public class JPADataSetProducer {

    @Produces
    @JPADataSet
    public JPADataSetExecutor produce(InjectionPoint ip){
        JPADataSet jpaDataSet = ip.getAnnotated().getAnnotation(JPADataSet.class);
        if(jpaDataSet.value() == null || jpaDataSet.value().trim().equals("")
                || jpaDataSet.unitName() == null || jpaDataSet.unitName().trim().equals("")){
            throw new RuntimeException("Invalid JPA dataset. Provide dataset value and unitName");
        }
        EntityManager em = EntityManagerProvider.instance(jpaDataSet.unitName()).em();
        em.clear();
        JPADataSetExecutor executor = JPADataSetExecutor.instance(UUID.randomUUID().toString().replaceAll("-", ""), em);
        DataSetModel dsModel = new DataSetModel(jpaDataSet.value()).disableConstraints(jpaDataSet.disableConstraints()).
                executeStatementsAfter(jpaDataSet.executeStatementsAfter()).executeStatementsBefore(jpaDataSet.executeStatementsBefore()).
                seedStrategy(jpaDataSet.strategy()).tableOrdering(jpaDataSet.tableOrdering()).
                useSequenceFiltering(jpaDataSet.useSequenceFiltering());
        executor.setDataSetModel(dsModel);

        return executor;
    }
}
