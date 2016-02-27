package com.github.dbunit.rules.cdi;

import com.github.dbunit.rules.cdi.api.UsingDataSet;

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
            dataSetProcessor.process(usingDataSet);
            proceed = invocationContext.proceed();
            if(usingDataSet.cleanAfter()){
                dataSetProcessor.clearDatabase(usingDataSet);
            }

            if (!"".equals(usingDataSet.executeCommandsAfter())) {
                dataSetProcessor.executeCommands(usingDataSet.executeCommandsAfter());
            }

            if(usingDataSet.executeScriptsAfter().length > 0){
                for (int i = 0; i < usingDataSet.executeScriptsAfter().length; i++) {
                    dataSetProcessor.executeScript(usingDataSet.executeScriptsAfter()[i]);

                }
            }
        }


        return proceed;
    }


}
