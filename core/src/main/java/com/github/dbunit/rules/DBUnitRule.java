package com.github.dbunit.rules;

import com.github.dbunit.rules.api.connection.ConnectionHolder;
import com.github.dbunit.rules.api.dataset.DataSet;
import com.github.dbunit.rules.api.dataset.DataSetExecutor;
import com.github.dbunit.rules.api.dataset.DataSetModel;
import com.github.dbunit.rules.api.dataset.ExpectedDataSet;
import com.github.dbunit.rules.connection.ConnectionHolderImpl;
import com.github.dbunit.rules.dataset.DataSetExecutorImpl;
import com.github.dbunit.rules.exception.DataBaseSeedingException;
import com.github.dbunit.rules.util.EntityManagerProvider;
import org.dbunit.DatabaseUnitException;
import org.dbunit.dataset.IDataSet;
import org.junit.Assert;
import org.junit.internal.runners.statements.ExpectException;
import org.junit.internal.runners.statements.Fail;
import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.logging.Logger;

import static com.github.dbunit.rules.util.EntityManagerProvider.em;
import static com.github.dbunit.rules.util.EntityManagerProvider.isEntityManagerActive;

/**
 * Created by rafael-pestano on 22/07/2015.
 */
public class DBUnitRule implements MethodRule {


  private String currentMethod;

  private DataSetExecutor executor;

  private DBUnitRule() {
  }

  public final static DBUnitRule instance(Connection connection) {
    return instance(new ConnectionHolderImpl(connection));
  }

  public final static DBUnitRule instance(String executorName, Connection connection) {
    return instance(executorName,new ConnectionHolderImpl(connection));
  }

  public final static DBUnitRule instance(ConnectionHolder connectionHolder) {
    return instance(DataSetExecutorImpl.DEFAULT_EXECUTOR_ID,connectionHolder);
  }

  public final static DBUnitRule instance(String executorName, ConnectionHolder connectionHolder) {

    DBUnitRule instance = new DBUnitRule();
    instance.init(executorName, connectionHolder);
    return instance;
  }


  @Override
  public Statement apply(final Statement statement, final FrameworkMethod frameworkMethod, Object o){
    currentMethod = frameworkMethod.getName();
    DataSet dataSet = frameworkMethod.getAnnotation(DataSet.class);
    if(dataSet == null){
      dataSet = frameworkMethod.getDeclaringClass().getAnnotation(DataSet.class);
    }
    if(dataSet != null) {
      final DataSetModel model = new DataSetModel().from(dataSet);
      final String datasetExecutorId = model.getExecutorId();
      boolean executorNameIsProvided = datasetExecutorId != null && !"".equals(datasetExecutorId.trim());
      if (executorNameIsProvided && !executor.getId().equals(datasetExecutorId)) {
        //we can have multiple @Rule so multiple executors on top of same dataset
        return statement;
      } else if (executorNameIsProvided) {
        executor = DataSetExecutorImpl.getExecutorById(datasetExecutorId);
      }
      try {
        executor.createDataSet(model);
      }catch (final Exception e){
        return new Fail(e);
      }
      return new Statement() {

        @Override
        public void evaluate() throws Throwable {
          boolean isTransactional = false;
          try {
            isTransactional = model.isTransactional() && isEntityManagerActive();
            if (isTransactional) {
              em().getTransaction().begin();
            }
            statement.evaluate();
            if (isTransactional) {
              em().getTransaction().commit();
            }
            performDataSetComparison(frameworkMethod);
          }catch (Exception e){
            if(isTransactional){
              em().getTransaction().rollback();
              throw e;
            }
          } finally {

            if (model != null && model.getExecuteStatementsAfter() != null && model.getExecuteStatementsAfter().length > 0) {
              try {
                executor.executeStatements(model.getExecuteStatementsAfter());
              } catch (Exception e) {
                LoggerFactory.getLogger(getClass().getName()).error(currentMethod + "() - Could not execute statements after:" + e.getMessage(), e);
              }
            }//end execute statements
            if (model != null && model.getExecuteScriptsAfter() != null && model.getExecuteScriptsAfter().length > 0) {
              try {
                for (int i = 0; i < model.getExecuteScriptsAfter().length; i++) {
                  executor.executeScript(model.getExecuteScriptsAfter()[i]);
                }
              } catch (Exception e) {
                if(e instanceof DatabaseUnitException){
                  throw e;
                }
                LoggerFactory.getLogger(getClass().getName()).error(currentMethod + "() - Could not execute scriptsAfter:" + e.getMessage(), e);
              }
            }//end execute scripts

            if(model.isCleanAfter()){
              executor.clearDatabase(model);
            }
          }
        }

      };
    } //end if dataSet != null
    else{
      return new Statement() {
        @Override
        public void evaluate() throws Throwable {
          statement.evaluate();
          performDataSetComparison(frameworkMethod);
        }
      };
    }
  }

  private void performDataSetComparison(FrameworkMethod frameworkMethod) throws DatabaseUnitException {
    ExpectedDataSet expectedDataSet = frameworkMethod.getAnnotation(ExpectedDataSet.class);
    if(expectedDataSet  == null){
      //try to infer from class level annotation
      expectedDataSet = frameworkMethod.getDeclaringClass().getAnnotation(ExpectedDataSet.class);
    }
    if(expectedDataSet != null){
      executor.compareCurrentDataSetWith(new DataSetModel(expectedDataSet.value()).disableConstraints(true),expectedDataSet.ignoreCols());
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

  public DataSetExecutor getDataSetExecutor(){
      return executor;
  }


}