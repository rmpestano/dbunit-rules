package com.github.dbunit.rules;

import com.github.dbunit.rules.connection.ConnectionHolder;
import com.github.dbunit.rules.connection.ConnectionHolderImpl;
import com.github.dbunit.rules.dataset.*;
import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;
import org.slf4j.*;

import java.sql.Connection;

/**
 * Created by rafael-pestano on 22/07/2015.
 */
public class DBUnitRule implements MethodRule {


  private String currentMethod;

  private DataSetExecutor executor;

  private DBUnitRule() {
  }

  public final synchronized static DBUnitRule instance(Connection connection) {
    return instance(new ConnectionHolderImpl(connection));
  }

  public final synchronized static DBUnitRule instance(String executorName, Connection connection) {
    return instance(executorName,new ConnectionHolderImpl(connection));
  }

  public final synchronized static DBUnitRule instance(ConnectionHolder connectionHolder) {
    return instance(DataSetExecutor.DEFAULT_EXECUTOR_ID,connectionHolder);
  }

  public final synchronized static DBUnitRule instance(String executorName, ConnectionHolder connectionHolder) {

    DBUnitRule instance = new DBUnitRule();
    instance.init(executorName, connectionHolder);
    return instance;
  }


  @Override
  public Statement apply(final Statement statement, final FrameworkMethod frameworkMethod, Object o){
    currentMethod = frameworkMethod.getName();
    final DataSet dataSet = frameworkMethod.getAnnotation(DataSet.class);
    final DataSetModel model = new DataSetModel().from(dataSet);
    String datasetExecutorName = model.getExecutorName();
    boolean executorNameIsProvided = datasetExecutorName != null && !"".equals(datasetExecutorName.trim());
    if(executorNameIsProvided && !executor.getId().equals(datasetExecutorName)){
      return new Statement() {
        @Override
        public void evaluate() throws Throwable {
          //intentional
        }
      };
    } else if(executorNameIsProvided){
      executor = DataSetExecutor.getExecutorById(datasetExecutorName);
    }
    executor.execute(model);

     return new Statement() {

      @Override
      public void evaluate() throws Throwable {
        try {
          statement.evaluate();
        }finally {
          if(model != null && model.getExecuteStatementsAfter() != null && model.getExecuteStatementsAfter().length > 0){
            try {
              executor.executeStatements(model.getExecuteStatementsAfter());
            }catch (Exception e){
              LoggerFactory.getLogger(getClass().getName()).error(currentMethod + "() - Could not execute statements after:" + e.getMessage(), e);
            }
          }
        }
      }

    };
  }

  private void init(String name, ConnectionHolder connectionHolder) {

    DataSetExecutor instance = DataSetExecutor.getExecutorById(name);
    if(instance == null){
      instance = DataSetExecutor.instance(name,connectionHolder);
      DataSetExecutor.getExecutors().put(name,instance);
    }
    executor = instance;

  }


}