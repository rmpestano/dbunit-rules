package com.github.dbunit.rules.dataset;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

import com.github.dbunit.rules.api.dataset.*;
import com.github.dbunit.rules.replacer.JSReplacer;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dbunit.rules.api.connection.ConnectionHolder;
import com.github.dbunit.rules.replacer.DateTimeReplacer;

/**
 * Created by pestano on 26/07/15.
 */
public class DataSetExecutorImpl implements DataSetExecutor {

    public static final String DEFAULT_EXECUTOR_ID = "default";

    private static Map<String, DataSetExecutorImpl> executors = new ConcurrentHashMap<>();

    private DatabaseConnection databaseConnection;

    private ConnectionHolder connectionHolder;

    private String id;

    private static final Logger log = LoggerFactory.getLogger(DataSetExecutorImpl.class);

    private DataSetModel dataSetModel;

    public static DataSetExecutorImpl instance(ConnectionHolder connectionHolder) {

        if (connectionHolder == null) {
            throw new RuntimeException("Invalid connection");
        }
        //if no executor name is provided use default
        return instance(DEFAULT_EXECUTOR_ID, connectionHolder);
    }

    public static DataSetExecutorImpl instance(String executorId, ConnectionHolder connectionHolder) {

        if (connectionHolder == null) {
            throw new RuntimeException("Invalid connection");
        }
        DataSetExecutorImpl instance = executors.get(executorId);
        if (instance == null) {
            instance = new DataSetExecutorImpl(executorId, connectionHolder);
            log.debug("creating executor instance " + executorId);
            executors.put(executorId, instance);
        }
        return instance;
    }

    private DataSetExecutorImpl(String executorId, ConnectionHolder connectionHolder) {
        this.connectionHolder = connectionHolder;
        this.id = executorId;
    }

    @Override
    public void createDataSet() {
        this.createDataSet(dataSetModel);
    }

    @Override
    public void createDataSet(DataSetModel dataSetModel) {
        if (dataSetModel != null && dataSetModel.getName() != null) {
            if (!dataSetModel.getName().contains(".")) {
                log.error("Dataset " + dataSetModel.getName() + "does not have extension");
            }
            DatabaseOperation operation = dataSetModel.getSeedStrategy().getOperation();

            String[] dataSets = dataSetModel.getName().trim().split(",");

            IDataSet target = null;
            String dataSetName = null;
            try {
                initDatabaseConnection();
                if (dataSetModel.isDisableConstraints()) {
                    disableConstraints();
                }
                if (dataSetModel.isCleanBefore()) {
                    try {
                        clearDatabase(dataSetModel);
                    } catch (SQLException e) {
                        LoggerFactory.getLogger(DataSetExecutorImpl.class.getName()).warn("Could not clean database before test.", e);
                    }
                }

                if (dataSetModel.getExecuteStatementsBefore() != null && dataSetModel.getExecuteStatementsBefore().length > 0) {
                    executeStatements(dataSetModel.getExecuteStatementsBefore());
                }

                if (dataSetModel.getExecuteScriptsBefore() != null && dataSetModel.getExecuteScriptsBefore().length > 0) {
                    for (int i = 0; i < dataSetModel.getExecuteScriptsBefore().length; i++) {
                        executeScript(dataSetModel.getExecuteScriptsBefore()[i]);
                    }
                }
                for (String dataSet : dataSets) {
                    dataSetName = dataSet.trim();
                    String extension = dataSetName.substring(dataSetName.lastIndexOf('.') + 1).toLowerCase();
                    switch (extension) {
                        case "yml": {
                            target = new YamlDataSet(loadDataSet(dataSetName));
                            break;
                        }
                        case "xml": {
                            target = new FlatXmlDataSetBuilder().build(loadDataSet(dataSetName));
                            break;
                        }
                        case "csv": {
                            target = new CsvDataSet(new File(getClass().getClassLoader().getResource(dataSetName).getFile()));
                            break;
                        }
                        case "xls": {
                            target = new XlsDataSet(loadDataSet(dataSetName));
                            break;
                        }
                        case "json": {
                            target = new JSONDataSet(loadDataSet(dataSetName));
                            break;
                        }
                        default:
                            log.error("Unsupported dataset extension");
                    }

                    if (target != null) {
                        target = performSequenceFiltering(dataSetModel, target);

                        target = performTableOrdering(dataSetModel, target);

                        target = performReplacements(target);

                        operation.execute(databaseConnection, target);
                    } else {
                        log.warn("DataSet not created" + dataSetName);
                    }

                }
            }catch (Exception e){
                log.error("Could not initialize dataset:" + e.getMessage(), e);
            }
        }
    }

    @Override
    public ConnectionHolder getConnectionHolder() {
        return connectionHolder;
    }

    private IDataSet performTableOrdering(DataSetModel dataSet, IDataSet target) throws AmbiguousTableNameException {
        if (dataSet.getTableOrdering().length > 0) {
            target = new FilteredDataSet(new SequenceTableFilter(dataSet.getTableOrdering()), target);
        }
        return target;
    }

    private IDataSet performSequenceFiltering(DataSetModel dataSet, IDataSet target) throws DataSetException, SQLException {
        if (dataSet.isUseSequenceFiltering()) {
            ITableFilter filteredTable = new DatabaseSequenceFilter(databaseConnection, target.getTableNames());
            target = new FilteredDataSet(filteredTable, target);
        }
        return target;
    }

    private void disableConstraints() throws SQLException {

        String driverName = connectionHolder.getConnection().getMetaData().getDriverName().toLowerCase();
        boolean isH2 = driverName.contains("hsql");
        if (isH2) {
            connectionHolder.getConnection().createStatement().execute("SET DATABASE REFERENTIAL INTEGRITY FALSE;");
        }

        boolean isMysql = driverName.contains("mysql");
        if (isMysql) {
            connectionHolder.getConnection().createStatement().execute(" SET FOREIGN_KEY_CHECKS=0;");
        }

        boolean isPostgres = driverName.contains("postgre");
        if (isPostgres) {
            connectionHolder.getConnection().createStatement().execute("SET CONSTRAINTS ALL DEFERRED;");
        }


    }

    public void executeStatements(String[] statements) {
        if (statements != null && statements.length > 0 && !"".equals(statements[0].trim())) {
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
                log.error("Could not createDataSet statements:" + e.getMessage(), e);
            }

        }
    }

    private IDataSet performReplacements(IDataSet dataSet) {
        IDataSet replace = DateTimeReplacer.replace(dataSet);
        replace = JSReplacer.replace(replace);
        return replace;
    }


    private void initDatabaseConnection() throws DatabaseUnitException {
        databaseConnection = new DatabaseConnection(connectionHolder.getConnection());
    }


    public void setConnectionHolder(ConnectionHolder connectionHolder) {
        this.connectionHolder = connectionHolder;
    }

    public Connection getConnection() {
        return connectionHolder.getConnection();
    }

    public static Map<String, DataSetExecutorImpl> getExecutors() {
        return executors;
    }


    @Override
    public boolean equals(Object other) {
        if (other instanceof DataSetExecutorImpl == false) {
            return false;
        }
        DataSetExecutorImpl otherExecutor = (DataSetExecutorImpl) other;
        if (databaseConnection == null || otherExecutor.databaseConnection == null) {
            return false;
        }
        try {
            if (databaseConnection.getConnection() == null || otherExecutor.databaseConnection.getConnection() == null) {
                return false;
            }

            if (!databaseConnection.getConnection().getMetaData().getURL().equals(otherExecutor.databaseConnection.getConnection().getMetaData().getURL())) {
                return false;
            }
        } catch (Exception e) {
            return false;
        }

        return true;
    }

    public String getId() {
        return id;
    }

    public static DataSetExecutorImpl getExecutorById(String id) {
        return executors.get(id);
    }

    private InputStream loadDataSet(String dataSet) {
        if (!dataSet.startsWith("/")) {
            dataSet = "/" + dataSet;
        }
        InputStream is = getClass().getResourceAsStream(dataSet);
        if (is == null) {//if not found try to get from datasets folder
            is = getClass().getResourceAsStream("/datasets" + dataSet);
        }
        return is;
    }

    /**
     * @throws SQLException
     */
    public void clearDatabase(DataSetModel dataset) throws SQLException {
        Connection connection = connectionHolder.getConnection();
        if (isHSqlDB()) {
            connection.createStatement().execute("TRUNCATE SCHEMA public AND COMMIT;");
        } else {

            if (dataset.getTableOrdering() != null && dataset.getTableOrdering().length > 0) {
                for (String table : dataset.getTableOrdering()) {
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

            result = metaData.getTables(null, null, "%", new String[]{"TABLE"});

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

    public void executeScript(String scriptPath) {
        if (scriptPath != null && !"".equals(scriptPath)) {
            if (!scriptPath.startsWith("/")) {
                scriptPath = "/" + scriptPath;
            }
            URL resource = getClass().getResource(scriptPath.trim());
            String absolutePath = "";
            if (resource != null) {
                absolutePath = resource.getPath();
            } else {
                resource = getClass().getResource("/scripts" + scriptPath.trim());
                if (resource != null) {
                    absolutePath = resource.getPath();
                }
            }
            if (resource == null) {
                log.error(String.format("Could not find script %s in classpath", scriptPath));
            }

            File scriptFile = new File(Paths.get(absolutePath).toUri());

            String[] scriptsStatements = readScriptStatements(scriptFile);
            if (scriptsStatements != null && scriptsStatements.length > 0) {
                executeStatements(scriptsStatements);
            }
        }
    }

    private String[] readScriptStatements(File scriptFile) {
        RandomAccessFile rad = null;
        int lineNum = 0;
        try {
            rad = new RandomAccessFile(scriptFile, "r");
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
            log.warn(String.format("Could not read script file %s. Error in line %d.", scriptFile.getAbsolutePath(), lineNum), e);
            return null;
        } finally {
            if (rad != null) {
                try {
                    rad.close();
                } catch (IOException e) {
                    log.warn("Could not close script file " + scriptFile.getAbsolutePath());

                }
            }
        }

    }

    @Override
    public void setDataSetModel(DataSetModel dataSetModel) {
        this.dataSetModel = dataSetModel;
    }

    @Override
    public DataSetModel getDataSetModel() {
        return dataSetModel;
    }

    public boolean isHSqlDB() throws SQLException {

        return connectionHolder.getConnection() != null && connectionHolder.getConnection().getMetaData().getDriverName().toLowerCase().contains("hsql");
    }
}
