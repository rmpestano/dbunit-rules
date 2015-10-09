package com.github.dbunit.rules.cdi;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.PostConstruct;
import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;

import com.github.dbunit.rules.cdi.api.UsingDataSet;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.AmbiguousTableNameException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.DatabaseSequenceFilter;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.FilteredDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.csv.CsvDataSet;
import org.dbunit.dataset.excel.XlsDataSet;
import org.dbunit.dataset.filter.ITableFilter;
import org.dbunit.dataset.filter.SequenceTableFilter;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.hibernate.Session;
import org.hibernate.internal.SessionImpl;

/**
 * Created by rafael-pestano on 08/10/2015.
 */
@RequestScoped
public class DataSetProcessor {

  private static final Logger log = Logger.getLogger(DataSetProcessor.class.getName());

  @Inject
  private EntityManager em;

  private Connection connection;

  private IDatabaseConnection databaseConnection;

  @PostConstruct
  public void init() {
    if (em == null) {
      throw new RuntimeException("Please provide an entity manager via CDI producer, see examples here: https://deltaspike.apache.org/documentation/jpa.html");
    }
    em.clear();
    this.connection = createConnection();
  }

  /**
   * unfortunately there is no standard way to get jdbc connection from JPA entity manager
   * @return JDBC connection
   */
  private Connection createConnection() {
    try {
      EntityTransaction tx = this.em.getTransaction();
      if (em.getDelegate() instanceof Session) {
        connection = ((SessionImpl) em.unwrap(Session.class)).connection();
      } else {
        /**
         * see here:http://wiki.eclipse.org/EclipseLink/Examples/JPA/EMAPI#Getting_a_JDBC_Connection_from_an_EntityManager
         */
        tx.begin();
        connection = em.unwrap(Connection.class);
        tx.commit();
      }
    } catch (Exception e) {
      throw new RuntimeException("Could not create database connection", e);
    }

    return connection;
  }

  public void process(UsingDataSet usingDataSet) {

    if (usingDataSet == null || usingDataSet.value() == null) {
      throw new RuntimeException("Provide dataset name");
    }
    String[] dataSets = usingDataSet.value().trim().split(",");

      try {
        initConn();

        if(usingDataSet.disableConstraints()){
          disableConstraints();
        }
        if (usingDataSet.cleanBefore()) {
          clearDatabase();
        }
        if(usingDataSet.executeCommandsBefore().length > 0){
          executeCommands(usingDataSet.executeCommandsBefore());
        }
        IDataSet target = null;
        for (String dataSet : dataSets) {
          dataSet = dataSet.trim();
          if (!dataSet.contains(".")) {
            throw new RuntimeException("Dataset " + dataSet + "does not have extension");
          }
          String extension = dataSet.substring(dataSet.lastIndexOf('.') + 1).toLowerCase();
          switch (extension) {
            case "yml": {
              target = new YamlDataSet(loadDataSet(dataSet));
              break;
            }
            case "xml": {
              target = new FlatXmlDataSetBuilder().build(loadDataSet(dataSet));
              break;
            }
            case "csv": {
              target = new CsvDataSet(new File(getClass().getClassLoader().getResource(dataSet).getFile()));
              break;
            }
            case "xls": {
              target = new XlsDataSet(loadDataSet(dataSet));
              break;
            }
            case "json": {
              target = new JSONDataSet(loadDataSet(dataSet));
              break;
            }
            default:
              throw new RuntimeException("Unsupported dataset extension");
          }
          if (target != null) {
            performReplacements(target);

            performTableOrdering(target,usingDataSet.tableOrdering());

            if(usingDataSet.useSequenceFiltering()){
              target = performSequenceFiltering(target);
            }
            usingDataSet.seedStrategy().getValue().execute(databaseConnection, target);
          } else {
            log.warning("DataSet not created" + dataSet);
          }
        }
      } catch (DatabaseUnitException e) {
        throw new RuntimeException("Could not initialize dataset:" + e.getMessage(), e);
      } catch (SQLException e) {
        throw new RuntimeException("Could not initialize dataset:" + e.getMessage(), e);
      } catch (IOException e) {
        throw new RuntimeException("Could not initialize dataset:" + e.getMessage(), e);
      }

  }

  private IDataSet performTableOrdering(IDataSet target, String[] tableOrdering) throws AmbiguousTableNameException {
    if (tableOrdering.length > 0 && !"".equals(tableOrdering[0])) {
      target = new FilteredDataSet(new SequenceTableFilter(tableOrdering), target);
    }
    return target;
  }

  private IDataSet performSequenceFiltering(IDataSet target) throws DataSetException, SQLException {
    ITableFilter filteredTable = new DatabaseSequenceFilter(databaseConnection,target.getTableNames());
    target = new FilteredDataSet(filteredTable, target);
    return target;
  }

  private IDataSet performReplacements(IDataSet dataSet) {
    return DateTimeReplacer.replace(dataSet);
  }



  public void executeCommands(String[] commands) {
    boolean hasCommands = commands.length > 0 && !"".equals(commands[0]);
    if(hasCommands){
      try {
        boolean autoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        java.sql.Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
            ResultSet.CONCUR_UPDATABLE);
        for (String stm : commands) {
          statement.addBatch(stm);
        }
        statement.executeBatch();
        connection.commit();
        connection.setAutoCommit(autoCommit);
      } catch (Exception e) {
        log.log(Level.WARNING, "Could not createDataSet statements:" + e.getMessage(), e);
      }
    }
  }

  private InputStream loadDataSet(String dataSet) {
    InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(dataSet);
    if (is == null) {//se nao encontrou tenta buscar na pasta datasets
      is = Thread.currentThread().getContextClassLoader().getResourceAsStream("datasets/" + dataSet);
    }
    return is;
  }

  private void initConn() throws DatabaseUnitException {
    databaseConnection = new DatabaseConnection(connection);
  }

  /**
   * @throws SQLException
   */
  private void clearDatabase() throws SQLException {
    if(isHSqlDB()){
      connection.createStatement().execute("TRUNCATE SCHEMA public AND COMMIT;");
    } else{
      //brute force approach
      ResultSet result = null;
      List<String> tables = getTableNames(connection);

      for (String tableName : tables) {
        connection.createStatement().executeUpdate("DELETE FROM " + tableName + " where 1=1");
        connection.commit();
      }
    }

  }

  private List<String> getTableNames(Connection con) {

    List<String> tables = new ArrayList<String>();
    ResultSet result = null;
    try {
      DatabaseMetaData metaData = con.getMetaData();

      result = metaData.getTables(null, null, "%", new String[] { "TABLE" });

      while (result.next()) {
        tables.add(result.getString("TABLE_NAME"));
      }

      return tables;
    } catch (SQLException ex) {
      log.log(Level.WARNING, "An exception occured while trying to"
          + "analyse the database.", ex);
      return new ArrayList<String>();
    }
  }

  private void disableConstraints() throws SQLException {

    String driverName = connection.getMetaData().getDriverName().toLowerCase();
    if (isHSqlDB()) {
      connection.createStatement().execute("SET DATABASE REFERENTIAL INTEGRITY FALSE;");
    }

    boolean isMysql = driverName.contains("mysql");
    if (isMysql) {
      connection.createStatement().execute(" SET FOREIGN_KEY_CHECKS=0;");
    }

    boolean isPostgres = driverName.contains("postgre");
    if (isPostgres) {
      connection.createStatement().execute("SET CONSTRAINTS ALL DEFERRED;");
    }

  }

  public boolean isHSqlDB() throws SQLException {
    return connection != null && connection.getMetaData().getDriverName().toLowerCase().contains("hsql");
  }

}