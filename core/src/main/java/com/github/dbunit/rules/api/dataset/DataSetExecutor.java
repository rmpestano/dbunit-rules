package com.github.dbunit.rules.api.dataset;

import com.github.dbunit.rules.api.connection.ConnectionHolder;
import org.dbunit.DatabaseUnitException;
import org.dbunit.dataset.IDataSet;

import java.sql.SQLException;

/**
 * Created by pestano on 01/08/15.
 */
public interface DataSetExecutor{

    /**
     * creates a dataset into executor's database connection using given dataSetModel
     * @param dataSetModel
     *
     * @return created IDataSet or null if none is created
     */
    IDataSet createDataSet(DataSetModel dataSetModel);

    ConnectionHolder getConnectionHolder();

    void clearDatabase(DataSetModel dataset) throws SQLException;

    void executeStatements(String[] statements);

    void executeScript(String scriptPath);

    String getId();

    /**
     * compares dataset from executor's databse connection with a given dataset
     * @param expected
     * @throws DatabaseUnitException if current dataset is not equal current dataset
     */
    void compareCurrentDataSetWith(IDataSet expected, String[] ignoreCols) throws DatabaseUnitException;
}
