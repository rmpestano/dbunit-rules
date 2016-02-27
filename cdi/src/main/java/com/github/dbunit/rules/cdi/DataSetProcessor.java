package com.github.dbunit.rules.cdi;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by rafael-pestano on 08/10/2015.
 */
@RequestScoped
public class DataSetProcessor {

  private static final Logger log = LoggerFactory.getLogger(DataSetProcessor.class.getName());

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
      if (isHibernatePresentOnClasspath() && em.getDelegate() instanceof Session) {
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
          clearDatabase(usingDataSet);
        }

        if(usingDataSet.executeCommandsBefore().length > 0){
          executeCommands(usingDataSet.executeCommandsBefore());
        }

        if(usingDataSet.executeScriptsBefore().length > 0){
          for (int i = 0; i < usingDataSet.executeScriptsBefore().length; i++) {
            executeScript(usingDataSet.executeScriptsBefore()[i]);

          }
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
            log.warn("DataSet not created" + dataSet);
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
        log.warn("Could not createDataSet statements:" + e.getMessage(), e);
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
  public void clearDatabase(UsingDataSet usingDataSet) throws SQLException {
    if(isHSqlDB()){
      connection.createStatement().execute("TRUNCATE SCHEMA public AND COMMIT;");
    } else {

      if(usingDataSet.tableOrdering() != null && usingDataSet.tableOrdering().length > 0){
        for (String table : usingDataSet.tableOrdering()) {
          connection.createStatement().executeUpdate("DELETE FROM " + table + " where 1=1");
          connection.commit();
        }
      }
      //clear remaining tables in any order(if there are any, also no problem clearing again)
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
      log.warn("An exception occured while trying to"
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

  public void executeScript(String scriptPath){
    URL resource = Thread.currentThread().getContextClassLoader().getResource(scriptPath.trim());
    String absolutePath = "";
    if(resource != null){
      absolutePath = resource.getPath();
    } else{
      resource = Thread.currentThread().getContextClassLoader().getResource("scripts/"+scriptPath.trim());
      if(resource != null){
        absolutePath = resource.getPath();
      }
    }
    if(resource == null){
      throw new RuntimeException(String.format("Could not find script %s in classpath",scriptPath));
    }

    File scriptFile  = new File(Paths.get(absolutePath).toUri());

    String[] scriptsStatements = readScriptStatements(scriptFile);
    if(scriptsStatements != null && scriptsStatements.length > 0){
      executeCommands(scriptsStatements);
    }
  }

  private String[] readScriptStatements(File scriptFile) {
    RandomAccessFile rad = null;
    int lineNum = 0;
    try {
      rad = new RandomAccessFile(scriptFile,"r");
      String line;
      List<String> scripts = new ArrayList<>();
      while ((line = rad.readLine()) != null) {
        //a line can have multiple scripts separated by ;
        String[] lineScripts = line.split(";");
        for (int i = 0; i < lineScripts.length; i++) {
          scripts.add(lineScripts[i]);
        }
        lineNum++;
      }
      return scripts.toArray(new String[scripts.size()]);
    } catch (Exception e) {
      log.warn(String.format("Could not read script file %s. Error in line %d.",scriptFile.getAbsolutePath(),lineNum),e);
      return null;
    } finally {
      if(rad != null){
        try {
          rad.close();
        } catch (IOException e) {
          log.warn("Could not close script file "+scriptFile.getAbsolutePath());

        }
      }
    }

  }


  public boolean isHSqlDB() throws SQLException {
    return connection != null && connection.getMetaData().getDriverName().toLowerCase().contains("hsql");
  }

  private boolean isHibernatePresentOnClasspath(){
    try {
      Class.forName( "org.hibernate.Session" );
      return true;
    } catch( ClassNotFoundException e ) {
      return false;
    }
  }

}