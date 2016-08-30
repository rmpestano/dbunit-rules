package com.github.dbunit.rules.api.dbunit;

import java.util.HashMap;
import java.util.Map;

import org.dbunit.database.DatabaseConfig;

import com.github.dbunit.rules.dataset.DataSetExecutorImpl;

public class DBUnitConfigModel {
    
    private String executorId;
    
    private boolean cacheConnection = false;
    
    private boolean cacheTables = false;
    
    private Map<String,Object> dbunitProperties;

    public DBUnitConfigModel(String executor) {
        dbunitProperties = new HashMap<>();
        this.executorId = executor;
        if("".equals(this.executorId)){
            this.executorId = DataSetExecutorImpl.DEFAULT_EXECUTOR_ID;
        }
    } 
    
    
    public static DBUnitConfigModel from(DBUnitConfig dbUnitConfig){
        DBUnitConfigModel dbUnitConfigModel = new DBUnitConfigModel(dbUnitConfig.executor());
        
        dbUnitConfigModel.cacheConnection(dbUnitConfig.cacheConnection()).
            cacheTables(dbUnitConfig.cacheTableNames()).
            addDBUnitProperty(DatabaseConfig.FEATURE_BATCHED_STATEMENTS, dbUnitConfig.batchedStatements()).
            addDBUnitProperty(DatabaseConfig.PROPERTY_BATCH_SIZE, dbUnitConfig.batchSize()).
            addDBUnitProperty(DatabaseConfig.FEATURE_ALLOW_EMPTY_FIELDS, dbUnitConfig.allowEmptyFields()).
            addDBUnitProperty(DatabaseConfig.PROPERTY_FETCH_SIZE, dbUnitConfig.fetchSize()).
            addDBUnitProperty(DatabaseConfig.FEATURE_QUALIFIED_TABLE_NAMES, dbUnitConfig.qualifiedTableNames());
        
        if(!"".equals(dbUnitConfig.escapePattern())){
            dbUnitConfigModel.addDBUnitProperty(DatabaseConfig.PROPERTY_ESCAPE_PATTERN, dbUnitConfig.escapePattern());
        }
        
        return dbUnitConfigModel;
    }
    
    
    public DBUnitConfigModel cacheConnection(boolean cacheConnection){
        this.cacheConnection = cacheConnection;
        return this;
    }
    
    
    public DBUnitConfigModel cacheTables(boolean cacheTables){
        this.cacheTables = cacheTables;
        return this;
    }
    
    public DBUnitConfigModel addDBUnitProperty(String name, Object value){
        dbunitProperties.put(name,value);
        return this;
    }
    
    public boolean isCacheConnection() {
        return cacheConnection;
    }


    public boolean isCacheTables() {
        return cacheTables;
    }


    public Map<String, Object> getDbunitProperties() {
        return dbunitProperties;
    }
    
    public String getExecutorId() {
        return executorId;
    }
    

}
