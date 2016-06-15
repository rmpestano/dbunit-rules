package com.github.dbunit.rules.cdi;

import com.github.dbunit.rules.api.dataset.DataSetModel;
import com.github.dbunit.rules.api.dataset.ExpectedDataSet;
import com.github.dbunit.rules.cdi.api.UsingDataSet;
import org.dbunit.dataset.IDataSet;

import javax.inject.Inject;
import javax.interceptor.AroundInvoke;
import javax.interceptor.Interceptor;
import javax.interceptor.InvocationContext;
import java.io.Serializable;

/**
 * Created by pestano on 26/07/15.
 */
@Interceptor
@UsingDataSet
public class DBUnitInterceptor implements Serializable {

    @Inject
    DataSetProcessor dataSetProcessor;

    @AroundInvoke
    public Object intercept(InvocationContext invocationContext)
            throws Exception {

        Object proceed = null;
        UsingDataSet usingDataSet = invocationContext.getMethod().getAnnotation(UsingDataSet.class);
        if (usingDataSet != null) {
            if (usingDataSet == null || usingDataSet.value() == null) {
                throw new RuntimeException("Provide dataset name(s).");
            }
            DataSetModel dataSetModel = new DataSetModel(usingDataSet.value()).
                    cleanAfter(usingDataSet.cleanAfter()).
                    cleanBefore(usingDataSet.cleanBefore()).
                    disableConstraints(usingDataSet.disableConstraints()).
                    executeScripsBefore(usingDataSet.executeScriptsBefore()).
                    executeScriptsAfter(usingDataSet.executeCommandsAfter()).
                    executeStatementsAfter(usingDataSet.executeCommandsAfter()).
                    executeStatementsBefore(usingDataSet.executeCommandsBefore()).
                    seedStrategy(usingDataSet.seedStrategy()).
                    tableOrdering(usingDataSet.tableOrdering()).
                    useSequenceFiltering(usingDataSet.useSequenceFiltering());
            dataSetProcessor.process(dataSetModel);
            proceed = invocationContext.proceed();
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
        }


        return proceed;
    }


}
