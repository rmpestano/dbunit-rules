package com.github.dbunit.rules.dataset;

import com.github.dbunit.rules.api.connection.ConnectionHolder;
import com.github.dbunit.rules.api.dataset.DataSetExecutor;
import com.github.dbunit.rules.api.dataset.DataSetModel;
import com.github.dbunit.rules.api.dataset.JSONDataSet;
import com.github.dbunit.rules.api.dataset.YamlDataSet;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
    public void createDataSet(){
        this.createDataSet(dataSetModel);
    }

    @Override
    public void createDataSet(DataSetModel dataSetModel) {
        if (dataSetModel != null && dataSetModel.getName() != null) {
            DatabaseOperation operation = dataSetModel.getSeedStrategy().getOperation();

            String[] dataSets = dataSetModel.getName().trim().split(",");

            IDataSet target = null;
            String dataSetName = null;
            try {
                initDatabaseConnection();
                if (dataSetModel.isDisableConstraints()) {
                    disableConstraints();
                }
                if (dataSetModel.getExecuteStatementsBefore() != null && dataSetModel.getExecuteStatementsBefore().length > 0) {
                    executeStatements(dataSetModel.getExecuteStatementsBefore());
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
                            log.error("Unsupported dataset extension" + extension);
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


            } catch (Exception e) {
                log.error("Could not create dataSet " + dataSetName, e);
            }

        } else {
            log.error("No dataset name was provided");
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
            ITableFilter filteredTable = new DatabaseSequenceFilter(databaseConnection);
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

    private IDataSet performReplacements(IDataSet dataSet) {
        return DateTimeReplacer.replace(dataSet);
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
        DataSetExecutorImpl executor = executors.get(id);
        if (executor == null) {
            LoggerFactory.getLogger(DataSetExecutorImpl.class.getName()).warn("No executor found with id " + id + ". Falling back to default executor");
            executor = executors.get(DataSetExecutorImpl.DEFAULT_EXECUTOR_ID);
        }
        return executor;
    }

    private InputStream loadDataSet(String dataSet) {
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(dataSet);
        if(is == null){//if not found try to get from datasets folder
            is = Thread.currentThread().getContextClassLoader().getResourceAsStream("datasets/" + dataSet);
        }
        return is;
    }

    @Override
    public void setDataSetModel(DataSetModel dataSetModel) {
        this.dataSetModel = dataSetModel;
    }

    @Override
    public DataSetModel getDataSetModel() {
        return dataSetModel;
    }
}
