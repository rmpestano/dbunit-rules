package com.github.dbunit.rules;

import com.github.dbunit.rules.connection.ConnectionHolder;
import com.github.dbunit.rules.connection.ConnectionHolderImpl;
import com.github.dbunit.rules.dataset.DataSet;
import com.github.dbunit.rules.dataset.JSONDataSet;
import com.github.dbunit.rules.dataset.YamlDataSet;
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

  private ConnectionHolder connectionHolder;

  private DatabaseConnection databaseConnection;

  private Logger log = LoggerFactory.getLogger(DBUnitRule.class);

  private String currentMethod;

  private static DBUnitRule instance;

  private DBUnitRule() {
  }

  public final static DBUnitRule instance(Connection connection) {
    if(instance == null){
      instance = new DBUnitRule();
    }
    instance.connectionHolder = new ConnectionHolderImpl(connection);
    return instance;
  }

  public final static DBUnitRule instance(ConnectionHolder connectionHolder) {

    if(instance == null){
      instance = new DBUnitRule();
    }
    instance.connectionHolder = connectionHolder;
    return instance;
  }

  @Override
  public Statement apply(final Statement statement, final FrameworkMethod frameworkMethod, Object o){
    currentMethod = frameworkMethod.getName();
    final DataSet dataSet = frameworkMethod.getAnnotation(DataSet.class);
    if(dataSet != null && dataSet.value() != null){
      DatabaseOperation operation = dataSet.strategy().getOperation();
      String dataSetName = dataSet.value();
      IDataSet target = null;
      try {
        initDatabaseConnection();
        if(dataSet.disableConstraints()){
          disableConstraints();
        }
        if(dataSet.executeStatementsBefore() != null && dataSet.executeStatementsBefore().length > 0){

          executeStatements(dataSet.executeStatementsBefore());
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
            log.error(currentMethod + "() - Unsupported dataset extension" + extension);
        }

        if(target != null) {
          target = performSequenceFiltering(dataSet, target);

          target = performTableOrdering(dataSet, target);

          target = performReplacements(target);

          operation.execute(databaseConnection, target);
        } else{
          log.warn(currentMethod + "() - Dataset not created" + dataSetName);
        }


      } catch (Exception e) {
        log.error(currentMethod + "() - Could not create dataset " + dataSetName, e);
      }

    }
     return new Statement() {

      @Override
      public void evaluate() throws Throwable {
        try {
          statement.evaluate();
        }finally {
          if(dataSet != null && dataSet.executeStatementsAfter() != null && dataSet.executeStatementsAfter().length > 0){
            try {
              executeStatements(dataSet.executeStatementsAfter());
            }catch (Exception e){
              log.error(currentMethod + "() - Could not execute statements after:" + e.getMessage(), e);
            }
          }
        }
      }

    };
  }

  private IDataSet performTableOrdering(DataSet dataSet, IDataSet target) throws AmbiguousTableNameException {
    if(dataSet.tableOrdering().length > 0){
      target = new FilteredDataSet(new SequenceTableFilter(dataSet.tableOrdering()), target);
    }
    return target;
  }

  private IDataSet performSequenceFiltering(DataSet dataSet, IDataSet target) throws DataSetException, SQLException {
    if(dataSet.useSequenceFiltering()){
      ITableFilter filteredTable = new DatabaseSequenceFilter(databaseConnection);
      target = new FilteredDataSet(filteredTable,target);
    }
    return target;
  }

  private void disableConstraints() throws SQLException{

    String driverName = connectionHolder.getConnection().getMetaData().getDriverName().toLowerCase();
    boolean isH2 = driverName.contains("hsql");
    if(isH2){
      connectionHolder.getConnection().createStatement().execute("SET DATABASE REFERENTIAL INTEGRITY FALSE;");
    }

    boolean isMysql = driverName.contains("mysql");
    if(isMysql){
      connectionHolder.getConnection().createStatement().execute(" SET FOREIGN_KEY_CHECKS=0;");
    }

    boolean isPostgres = driverName.contains("postgre");
    if(isPostgres){
      connectionHolder.getConnection().createStatement().execute("SET CONSTRAINTS ALL DEFERRED;");
    }


  }

  private void executeStatements(String[] statements) {
    try {
      boolean autoCommit = connectionHolder.getConnection().getAutoCommit();
      connectionHolder.getConnection().setAutoCommit(false);
      java.sql.Statement statement = connectionHolder.getConnection().createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
              ResultSet.CONCUR_UPDATABLE);
      for (String stm : statements) {
        statement.addBatch(stm);
      }
      statement.executeBatch();
      connectionHolder.getConnection().commit();
      connectionHolder.getConnection().setAutoCommit(autoCommit);
    } catch (Exception e) {
      log.error(currentMethod + "() -Could not execute statements:" + e.getMessage(), e);
    }

  }

  private IDataSet performReplacements(IDataSet dataSet) {
    return DateTimeReplacer.replace(dataSet);
  }




  private void initDatabaseConnection() throws DatabaseUnitException {
    databaseConnection = new DatabaseConnection(connectionHolder.getConnection());
  }


  public void setConnectionHolder(ConnectionHolder connectionHolder) {
    this.connectionHolder = connectionHolder;
  }
}