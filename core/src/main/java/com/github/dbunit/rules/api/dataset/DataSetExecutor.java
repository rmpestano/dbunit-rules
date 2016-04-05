package com.github.dbunit.rules.api.dataset;

import com.github.dbunit.rules.api.connection.ConnectionHolder;

import java.sql.SQLException;

/**
 * Created by pestano on 01/08/15.
 */
public interface DataSetExecutor{

    /**
     * creates a dataset into executor's database connection using given dataSetModel
     * @param dataSetModel
     */
    void createDataSet(DataSetModel dataSetModel);

    /**
     * creates a dataset into executor's database connection using its dataset model instance
     * Note that dataset model instance should be set before calling this method.
     */
    void createDataSet();

    ConnectionHolder getConnectionHolder();

    void setDataSetModel(DataSetModel dataSetModel);

    DataSetModel getDataSetModel();

    void clearDatabase(DataSetModel dataset) throws SQLException;

    void executeStatements(String[] statements);

    void executeScript(String scriptPath);
}
