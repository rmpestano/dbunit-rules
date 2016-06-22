package com.github.dbunit.rules.cdi;

import com.github.dbunit.rules.api.dataset.DataSetModel;
import com.github.dbunit.rules.api.dataset.ExpectedDataSet;
import com.github.dbunit.rules.cdi.api.UsingDataSet;

import javax.inject.Inject;
import javax.interceptor.AroundInvoke;
import javax.interceptor.Interceptor;
import javax.interceptor.InvocationContext;
import javax.persistence.EntityManager;
import java.io.Serializable;

/**
 * Created by pestano on 26/07/15.
 */
@Interceptor
@UsingDataSet
public class DBUnitInterceptor implements Serializable {

    @Inject
    DataSetProcessor dataSetProcessor;

    @Inject
    private EntityManager em;

    @AroundInvoke
    public Object intercept(InvocationContext invocationContext)
            throws Exception {

        Object proceed = null;
        UsingDataSet usingDataSet = invocationContext.getMethod().getAnnotation(UsingDataSet.class);
        if (usingDataSet != null) {
            DataSetModel dataSetModel = new DataSetModel(usingDataSet.value()).
                    cleanAfter(usingDataSet.cleanAfter()).
                    cleanBefore(usingDataSet.cleanBefore()).
                    disableConstraints(usingDataSet.disableConstraints()).
                    executeScripsBefore(usingDataSet.executeScriptsBefore()).
                    executeScriptsAfter(usingDataSet.executeCommandsAfter()).
                    executeStatementsAfter(usingDataSet.executeCommandsAfter()).
                    executeStatementsBefore(usingDataSet.executeCommandsBefore()).
                    seedStrategy(usingDataSet.seedStrategy()).
                    transactional(usingDataSet.transactional()).
                    tableOrdering(usingDataSet.tableOrdering()).
                    useSequenceFiltering(usingDataSet.useSequenceFiltering());
            dataSetProcessor.process(dataSetModel);
            boolean isTransactionalTest = dataSetModel.isTransactional();
            if(isTransactionalTest){
                em.getTransaction().begin();
            }
            try {
                proceed = invocationContext.proceed();
                if(isTransactionalTest){
                    em.getTransaction().commit();
                }
            }catch (Exception e){
                if(isTransactionalTest){
                    em.getTransaction().rollback();
                }
                throw e;
            }
            ExpectedDataSet expectedDataSet = invocationContext.getMethod().getAnnotation(ExpectedDataSet.class);
            if(expectedDataSet != null){
                dataSetProcessor.compareCurrentDataSetWith(new DataSetModel(expectedDataSet.value()).disableConstraints(true),expectedDataSet.ignoreCols());
            }
            if(usingDataSet.cleanAfter()){
                dataSetProcessor.clearDatabase(dataSetModel);
            }

            if (!"".equals(usingDataSet.executeCommandsAfter())) {
                dataSetProcessor.executeStatements(dataSetModel.getExecuteStatementsAfter());
            }

            if(usingDataSet.executeScriptsAfter().length > 0 && !"".equals(usingDataSet.executeScriptsAfter()[0])){
                for (int i = 0; i < usingDataSet.executeScriptsAfter().length; i++) {
                    dataSetProcessor.executeScript(usingDataSet.executeScriptsAfter()[i]);
                }
            }
        } else{//no dataset provided, just proceed and check expectedDataSet
            proceed = invocationContext.proceed();
            ExpectedDataSet expectedDataSet = invocationContext.getMethod().getAnnotation(ExpectedDataSet.class);
            if(expectedDataSet != null){
                dataSetProcessor.compareCurrentDataSetWith(new DataSetModel(expectedDataSet.value()).disableConstraints(true),expectedDataSet.ignoreCols());
            }
        }


        return proceed;
    }


}
