package com.github.dbunit.rules;

import com.github.dbunit.rules.connection.ConnectionHolder;
import com.github.dbunit.rules.connection.ConnectionHolderImpl;
import com.github.dbunit.rules.dataset.*;
import com.github.dbunit.rules.replacer.DateTimeReplacer;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.AmbiguousTableNameException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.DatabaseSequenceFilter;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.FilteredDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.csv.CsvDataSet;
import org.dbunit.dataset.excel.XlsDataSet;
import org.dbunit.dataset.filter.ITableFilter;
import org.dbunit.dataset.filter.SequenceTableFilter;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.operation.DatabaseOperation;
import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;
import org.slf4j.*;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;


/**
 * Created by rafael-pestano on 22/07/2015.
 */
public class DBUnitRule implements MethodRule {


  private String currentMethod;

  private DataSetExecutor executor;

  private static DBUnitRule instance;

  private DBUnitRule() {
  }

  public final static DBUnitRule instance(Connection connection) {
    if(instance == null){
      instance = new DBUnitRule();
    }
    instance.executor = DataSetExecutor.instance(new ConnectionHolderImpl(connection));
    return instance;
  }

  public final static DBUnitRule instance(ConnectionHolder connectionHolder) {

    if(instance == null){
      instance = new DBUnitRule();
    }
    instance.executor = DataSetExecutor.instance(connectionHolder);
    return instance;
  }

  public static DBUnitRule currentInstance(){
    if(instance.executor == null){
      throw new RuntimeException("There is no instance to retrieve.");
    }
    return instance;
  }

  @Override
  public Statement apply(final Statement statement, final FrameworkMethod frameworkMethod, Object o){
    currentMethod = frameworkMethod.getName();
    final DataSet dataSet = frameworkMethod.getAnnotation(DataSet.class);
    final DataSetModel model = new DataSetModel().from(dataSet);
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

  public DataSetExecutor getExecutor() {
    return executor;
  }
}