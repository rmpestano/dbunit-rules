package com.gihub.dbunit.rules;

import com.gihub.dbunit.rules.dataset.JSONDataSet;
import com.gihub.dbunit.rules.dataset.UsingDataSet;
import com.gihub.dbunit.rules.dataset.YamlDataSet;
import com.gihub.dbunit.rules.replacer.DateTimeReplacer;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.csv.CsvDataSet;
import org.dbunit.dataset.excel.XlsDataSet;
import org.dbunit.dataset.xml.XmlDataSet;
import org.dbunit.operation.DatabaseOperation;
import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by rafael-pestano on 22/07/2015.
 */
public class DBUnitRule implements MethodRule {

  private Connection connection;

  private DatabaseConnection databaseConnection;

  private DBUnitRule(Connection conn) {
    this.connection = conn;
  }

  public final static DBUnitRule instance(Connection connection) {
    return new DBUnitRule(connection);
  }

  @Override
  public Statement apply(final Statement statement, FrameworkMethod frameworkMethod, Object o){

    UsingDataSet usingDataSet = frameworkMethod.getAnnotation(UsingDataSet.class);
    if(usingDataSet != null && usingDataSet.value() != null){
      String dataSet = usingDataSet.value();
      try {
        initConn();
        String extension = dataSet.substring(dataSet.lastIndexOf('.')+1).toLowerCase();
        switch (extension){
          case "yml": {
            DatabaseOperation.CLEAN_INSERT.execute(databaseConnection, performReplacements(new YamlDataSet(Thread.currentThread().getContextClassLoader().getResourceAsStream(dataSet))));
            break;
          }
          case "xml": {
            DatabaseOperation.CLEAN_INSERT.execute(databaseConnection, new XmlDataSet(Thread.currentThread().getContextClassLoader().getResourceAsStream(dataSet)));
            break;
          }
          case "csv": {
            DatabaseOperation.CLEAN_INSERT.execute(databaseConnection, new CsvDataSet(new File(getClass().getClassLoader().getResource(dataSet).getFile())));
            break;
          }
          case "xls": {
            DatabaseOperation.CLEAN_INSERT.execute(databaseConnection, new XlsDataSet(Thread.currentThread().getContextClassLoader().getResourceAsStream(dataSet)));
            break;
          }
          case "json": {
            DatabaseOperation.CLEAN_INSERT.execute(databaseConnection, new JSONDataSet(Thread.currentThread().getContextClassLoader().getResourceAsStream(dataSet)));
            break;
          }
          default:
            closeConn();
            throw new RuntimeException("Unsupported dataset extension" + extension);
        }

      } catch (DatabaseUnitException e) {
        throw new RuntimeException("Could not initialize dataset:"+e.getMessage(),e);
      } catch (SQLException e) {
        throw new RuntimeException("Could not initialize dataset:"+e.getMessage(),e);
      } catch (IOException e) {
        throw new RuntimeException("Could not initialize dataset:"+e.getMessage(),e);
      }

    }
     return new Statement() {

      @Override
      public void evaluate() throws Throwable {
        try {
          statement.evaluate();
        }finally {
          closeConn();
        }
      }

    };
  }

  private IDataSet performReplacements(IDataSet dataSet) {
    return DateTimeReplacer.replace(dataSet);
  }


  private void closeConn() {
    try {
      if (databaseConnection != null && !databaseConnection.getConnection().isClosed()) {
        databaseConnection.getConnection().close();
      }
    } catch (SQLException e) {
      e.printStackTrace();
      throw new RuntimeException("could not close conection \nmessage: " + e.getMessage(),e);
    }

  }

  private void initConn() throws DatabaseUnitException {
    databaseConnection = new DatabaseConnection(connection);
  }

}