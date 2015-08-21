package com.github.dbunit.rules;

import com.github.dbunit.rules.api.connection.ConnectionHolder;
import com.github.dbunit.rules.api.dataset.DataSet;
import com.github.dbunit.rules.api.dataset.DataSetModel;
import com.github.dbunit.rules.connection.ConnectionHolderImpl;
import com.github.dbunit.rules.dataset.DataSetExecutorImpl;
import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;
import org.slf4j.LoggerFactory;

import java.sql.Connection;

import static org.junit.Assert.fail;

/**
 * Created by rafael-pestano on 22/07/2015.
 */
public class DBUnitRule implements MethodRule {


  private String currentMethod;

  private DataSetExecutorImpl executor;

  private DBUnitRule() {
  }

  public final synchronized static DBUnitRule instance(Connection connection) {
    return instance(new ConnectionHolderImpl(connection));
  }

  public final synchronized static DBUnitRule instance(String executorName, Connection connection) {
    return instance(executorName,new ConnectionHolderImpl(connection));
  }

  public final synchronized static DBUnitRule instance(ConnectionHolder connectionHolder) {
    return instance(DataSetExecutorImpl.DEFAULT_EXECUTOR_ID,connectionHolder);
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
    if(dataSet != null) {
      final DataSetModel model = new DataSetModel().from(dataSet);
      final String datasetExecutorId = model.getExecutorId();
      boolean executorNameIsProvided = datasetExecutorId != null && !"".equals(datasetExecutorId.trim());
      if (executorNameIsProvided && !executor.getId().equals(datasetExecutorId)) {
        return new Statement() {
          @Override
          public void evaluate() throws Throwable {
            //intentional cause we can have multiple @Rule so multiple executors on top of same dataset
            LoggerFactory.getLogger(getClass().getName()).warn("Dataset executor: '" + datasetExecutorId + "' for method " + currentMethod + "() - does not match current executor id -> '" + executor.getId() + "'.\nIgnore this warn if you are using multiple executors");
          }
        };
      } else if (executorNameIsProvided) {
        executor = DataSetExecutorImpl.getExecutorById(datasetExecutorId);
      }
      executor.createDataSet(model);

      return new Statement() {

        @Override
        public void evaluate() throws Throwable {
          try {
            statement.evaluate();
          } finally {
            if (model != null && model.getExecuteStatementsAfter() != null && model.getExecuteStatementsAfter().length > 0) {
              try {
                executor.executeStatements(model.getExecuteStatementsAfter());
              } catch (Exception e) {
                LoggerFactory.getLogger(getClass().getName()).error(currentMethod + "() - Could not createDataSet statements after:" + e.getMessage(), e);
              }
            }
          }
        }

      };
    } //end if dataSet != null
    else{
      return new Statement() {
        @Override
        public void evaluate() throws Throwable {
          //intentional
        }
      };
    }
  }

  private void init(String name, ConnectionHolder connectionHolder) {

    DataSetExecutorImpl instance = DataSetExecutorImpl.getExecutorById(name);
    if(instance == null){
      instance = DataSetExecutorImpl.instance(name, connectionHolder);
      DataSetExecutorImpl.getExecutors().put(name,instance);
    }
    executor = instance;

  }


}