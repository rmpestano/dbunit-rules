package com.github.dbunit.rules.cdi;

import com.github.dbunit.rules.cdi.api.UsingDataSet;

import java.io.Serializable;

import javax.inject.Inject;
import javax.interceptor.AroundInvoke;
import javax.interceptor.Interceptor;
import javax.interceptor.InvocationContext;

/**
 * Created by pestano on 26/07/15.
 */
@Interceptor
@UsingDataSet
public class DBUnitInterceptor implements Serializable {

  @Inject
  DataSetProcessor dbUnitProcessor;

  @AroundInvoke
  public Object intercept(InvocationContext invocationContext)
      throws Exception {

    Object proceed = null;
    UsingDataSet usingDataSet = invocationContext.getMethod().getAnnotation(UsingDataSet.class);
    if (usingDataSet != null) {
      dbUnitProcessor.process(usingDataSet);
      proceed = invocationContext.proceed();
      if(!"".equals(usingDataSet.executeCommandsAfter())){
             dbUnitProcessor.executeCommands(usingDataSet.executeCommandsAfter());
           }
        }

        return proceed;
    }


}
