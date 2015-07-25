package com.github.dbunit.rules;

import com.github.dbunit.rules.dataset.DataSet;
import com.github.dbunit.rules.dataset.JSONDataSet;
import com.github.dbunit.rules.dataset.YamlDataSet;
import com.github.dbunit.rules.replacer.DateTimeReplacer;
import com.sun.istack.internal.logging.Logger;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.DatabaseSequenceFilter;
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

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;

/**
 * Created by rafael-pestano on 22/07/2015.
 */
public class DBUnitRule implements MethodRule {

  private Connection connection;

  private DatabaseConnection databaseConnection;

  private Logger log = Logger.getLogger(DBUnitRule.class);

  private static DBUnitRule instance;

  private DBUnitRule() {
  }

  public final static DBUnitRule instance(Connection connection) {
    if(instance == null){
      instance = new DBUnitRule();
    }
    instance.connection = connection;
    return instance;
  }

  @Override
  public Statement apply(final Statement statement, final FrameworkMethod frameworkMethod, Object o){

    final DataSet dataSet = frameworkMethod.getAnnotation(DataSet.class);
    if(dataSet != null && dataSet.value() != null){
      DatabaseOperation operation = dataSet.strategy().getOperation();
      String dataSetName = dataSet.value();
      IDataSet target = null;
      try {
        initConn();
        if(dataSet.disableConstraints()){
          disableConstraints();
        }
        if(dataSet.executeStatementsBefore() != null && dataSet.executeStatementsBefore().length > 0){

          executeStatements(dataSet.executeStatementsBefore(),frameworkMethod.getName());
        }
        String extension = dataSetName.substring(dataSetName.lastIndexOf('.')+1).toLowerCase();
        switch (extension){
          case "yml": {
            target = new YamlDataSet(Thread.currentThread().getContextClassLoader().getResourceAsStream(dataSetName));
            break;
          }
          case "xml": {
            target = new FlatXmlDataSetBuilder().build(Thread.currentThread().getContextClassLoader().getResourceAsStream(dataSetName));
            break;
          }
          case "csv": {
            target = new CsvDataSet(new File(getClass().getClassLoader().getResource(dataSetName).getFile()));
            break;
          }
          case "xls": {
            target = new XlsDataSet(Thread.currentThread().getContextClassLoader().getResourceAsStream(dataSetName));
            break;
          }
          case "json": {
           target = new JSONDataSet(Thread.currentThread().getContextClassLoader().getResourceAsStream(dataSetName));
            break;
          }
          default:
            closeConn();
            log.severe("Unsupported dataset extension" + extension);
        }

        if(target != null){
          if(dataSet.useSequenceFiltering()){
            ITableFilter filteredTable = new DatabaseSequenceFilter(databaseConnection);
            target = new FilteredDataSet(filteredTable,target);
          }
          if(dataSet.tableCreationOrder().length > 0){
            target = new FilteredDataSet(new SequenceTableFilter(dataSet.tableCreationOrder()), target);
          }

          target = performReplacements(target);
          operation.execute(databaseConnection, target);
        }

      } catch (Exception e) {
        closeConn();
        log.severe(frameworkMethod.getName() + "() - Could not create dataset " + dataSetName, e);
      }

    }
     return new Statement() {

      @Override
      public void evaluate() throws Throwable {
        try {
          statement.evaluate();
        }finally {
          if(dataSet.executeStatementsAfter() != null && dataSet.executeStatementsAfter().length > 0){
            try {
              executeStatements(dataSet.executeStatementsAfter(),frameworkMethod.getName());
            }catch (Exception e){
              log.log(Level.SEVERE, "Could not execute statements after:" + e.getMessage(), e);
            }
          }
          closeConn();
        }
      }

    };
  }

  private void disableConstraints() throws SQLException{

    String driverName = connection.getMetaData().getDriverName().toLowerCase();
    boolean isH2 = driverName.contains("hsql");
    if(isH2){
      connection.createStatement().execute("SET DATABASE REFERENTIAL INTEGRITY FALSE;");
    }

    boolean isMysql = driverName.contains("mysql");
    if(isMysql){
      connection.createStatement().execute(" SET FOREIGN_KEY_CHECKS=0;");
    }

    boolean isPostgres = driverName.contains("postgre");
    if(isPostgres){
      connection.createStatement().execute("SET CONSTRAINTS ALL DEFERRED;");
    }


  }

  private void executeStatements(String[] statements, String methodName) {
    try {
      boolean autoCommit = connection.getAutoCommit();
      connection.setAutoCommit(false);
      java.sql.Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
              ResultSet.CONCUR_UPDATABLE);
      for (String stm : statements) {
        statement.addBatch(stm);
      }
      statement.executeBatch();
      connection.commit();
      connection.setAutoCommit(autoCommit);
    } catch (Exception e) {
      log.log(Level.SEVERE,methodName + "() -Could not execute statements:" +e.getMessage(), e);
    }


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
      log.log(Level.SEVERE, "Cound not close connection:" + e.getMessage(), e);
    }

  }

  private void initConn() throws DatabaseUnitException {
    databaseConnection = new DatabaseConnection(connection);
  }

}