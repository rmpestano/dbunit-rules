package com.github.dbunit.rules.dataset;

import com.github.dbunit.rules.connection.ConnectionHolder;
import com.github.dbunit.rules.replacer.DateTimeReplacer;
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
import org.dbunit.operation.DatabaseOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * COPIED from JPA module because of maven cyclic dependencies (even with test scope)
 * Created by pestano on 26/07/15.
 */
public class DataSetExecutor {


    private static Map<String,DataSetExecutor> executors = new ConcurrentHashMap<>();

    private DatabaseConnection databaseConnection;

    private ConnectionHolder connectionHolder;

    private Logger log = LoggerFactory.getLogger(DataSetExecutor.class);



    public static DataSetExecutor instance(ConnectionHolder connectionHolder) {

        if(connectionHolder == null){
            throw new RuntimeException("Invalid connection");
        }
        DataSetExecutor instance =  new DataSetExecutor(connectionHolder);
            executors.put(UUID.randomUUID().toString(),instance);
        return instance;
    }

    public static DataSetExecutor instance(String instanceName, ConnectionHolder connectionHolder) {

        if(connectionHolder == null){
            throw new RuntimeException("Invalid connection");
        }
        DataSetExecutor instance = executors.get(instanceName);
        if(instance == null){
            instance = new DataSetExecutor(connectionHolder);
            executors.put(instanceName,instance);
        }
        return instance;
    }

    private DataSetExecutor(ConnectionHolder connectionHolder) {
        this.connectionHolder = connectionHolder;
    }

    public void execute(DataSetModel dataSetModel) {
        if(dataSetModel != null && dataSetModel.getName() != null){
            DatabaseOperation operation = dataSetModel.getSeedStrategy().getOperation();
            String dataSetName = dataSetModel.getName();
            IDataSet target = null;
            try {
                initDatabaseConnection();
                if(dataSetModel.isDisableConstraints()){
                    disableConstraints();
                }
                if(dataSetModel.getExecuteStatementsBefore() != null && dataSetModel.getExecuteStatementsBefore().length > 0){
                    executeStatements(dataSetModel.getExecuteStatementsBefore());
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
                        log.error("Unsupported dataset extension" + extension);
                }

                if(target != null) {
                    target = performSequenceFiltering(dataSetModel, target);

                    target = performTableOrdering(dataSetModel, target);

                    target = performReplacements(target);

                    operation.execute(databaseConnection, target);
                } else{
                    log.warn("DataSet not created" + dataSetName);
                }


            } catch (Exception e) {
                log.error("Could not create dataSet " + dataSetName, e);
            }

        } else{
            log.error("No dataset name was provided");
        }
    }

    private IDataSet performTableOrdering(DataSetModel dataSet, IDataSet target) throws AmbiguousTableNameException {
        if(dataSet.getTableOrdering().length > 0){
            target = new FilteredDataSet(new SequenceTableFilter(dataSet.getTableOrdering()), target);
        }
        return target;
    }

    private IDataSet performSequenceFiltering(DataSetModel dataSet, IDataSet target) throws DataSetException, SQLException {
        if(dataSet.isUseSequenceFiltering()){
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
            log.error("Could not execute statements:" + e.getMessage(), e);
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

    public Connection getConnection(){
        return connectionHolder.getConnection();
    }

    public static Map<String, DataSetExecutor> getExecutors() {
        return executors;
    }

    @Override
    public boolean equals(Object other) {
        if(other instanceof DataSetExecutor == false){
            return false;
        }
        DataSetExecutor otherExecutor = (DataSetExecutor) other;
        if(databaseConnection == null || otherExecutor.databaseConnection == null){
            return false;
        }
        try {
            if(databaseConnection.getConnection() == null || otherExecutor.databaseConnection.getConnection() == null){
                return false;
            }

            if(!databaseConnection.getConnection().getMetaData().getURL().equals(otherExecutor.databaseConnection.getConnection().getMetaData().getURL())){
                return false;
            }
        } catch (Exception e) {
            return false;
        }

        return true;
    }
}
