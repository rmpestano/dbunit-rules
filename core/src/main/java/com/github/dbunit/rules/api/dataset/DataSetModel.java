package com.github.dbunit.rules.api.dataset;

import com.github.dbunit.rules.dataset.DataSetExecutorImpl;

/**
 * Created by pestano on 26/07/15.
 */
public class DataSetModel {

    private String name;
    private String executorId = DataSetExecutorImpl.DEFAULT_EXECUTOR_ID;
    private SeedStrategy seedStrategy = SeedStrategy.CLEAN_INSERT;
    private boolean useSequenceFiltering = true;
    private boolean disableConstraints = false;
    private boolean cleanBefore = false;
    private boolean cleanAfter = false;
    private String[] tableOrdering = {};
    private String[] executeStatementsBefore = {};
    private String[] executeStatementsAfter = {};


    public DataSetModel() {
    }

    public DataSetModel(String name) {
        this.name = name;
    }

    public DataSetModel name(String name) {
        this.name = name;
        return this;
    }

    public DataSetModel seedStrategy(SeedStrategy seedStrategy) {
        this.seedStrategy = seedStrategy;
        return this;
    }

    public DataSetModel useSequenceFiltering(boolean useSequenceFiltering) {
        this.useSequenceFiltering = useSequenceFiltering;
        return this;
    }

    public DataSetModel disableConstraints(boolean disableConstraints) {
        this.disableConstraints = disableConstraints;
        return this;
    }

    public DataSetModel tableOrdering(String[] tableOrdering) {
        this.tableOrdering = tableOrdering;
        return this;
    }

    public DataSetModel cleanBefore(boolean cleanBefore) {
        this.cleanBefore = cleanBefore;
        return this;
    }

    public DataSetModel cleanAfter(boolean cleanAfter) {
        this.cleanAfter = cleanAfter;
        return this;
    }

    public DataSetModel executeStatementsBefore(String[] executeStatementsBefore) {
        this.executeStatementsBefore = executeStatementsBefore;
        return this;
    }

    public DataSetModel executeStatementsAfter(String[] executeStatementsAfter) {
        this.executeStatementsAfter = executeStatementsAfter;
        return this;
    }

    /**
     *  name of dataset executor for the given dataset. If not specified the default one will be used.
     *
     * Use this option to work with multple database conncetions. Remember that each executor has its own connection.
     * @return datasetModel with executor name configured
     */
    public DataSetModel executorId(String executorId) {
        this.executorId = executorId;
        return this;
    }


    public DataSetModel from(DataSet dataSet) {
        if(dataSet != null){
            return name(dataSet.value()).seedStrategy(dataSet.strategy()).
                    useSequenceFiltering(dataSet.useSequenceFiltering()).
                    tableOrdering(dataSet.tableOrdering()).
                    disableConstraints(dataSet.disableConstraints()).
                    executorId(dataSet.executorId()).
                    executeStatementsBefore(dataSet.executeStatementsBefore()).
                    cleanBefore(dataSet.cleanBefore()).
                    cleanAfter(dataSet.cleanAfter()).
                    executeStatementsAfter(dataSet.executeStatementsAfter());
        } else{
            throw new RuntimeException("Cannot create DataSetModel from Null DataSet");
        }

    }




    public String getName() {
        return name;
    }

    public SeedStrategy getSeedStrategy() {
        return seedStrategy;
    }

    public boolean isUseSequenceFiltering() {
        return useSequenceFiltering;
    }

    public boolean isDisableConstraints() {
        return disableConstraints;
    }

    public String[] getTableOrdering() {
        return tableOrdering;
    }

    public String[] getExecuteStatementsBefore() {
        return executeStatementsBefore;
    }

    public String[] getExecuteStatementsAfter() {
        return executeStatementsAfter;
    }

    public String getExecutorId() {
        return executorId;
    }

    public boolean isCleanBefore() {
        return cleanBefore;
    }

    public boolean isCleanAfter() {
        return cleanAfter;
    }
}
