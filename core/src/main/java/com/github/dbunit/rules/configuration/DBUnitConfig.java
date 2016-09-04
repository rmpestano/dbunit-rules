package com.github.dbunit.rules.configuration;

import com.github.dbunit.rules.api.configuration.DBUnit;
import com.github.dbunit.rules.dataset.DataSetExecutorImpl;
import org.dbunit.database.DatabaseConfig;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * represents DBUnit configuration of a dataset executor.
 */
public class DBUnitConfig {
    
    private String executorId;
    
    private boolean cacheConnection = false;
    
    private boolean cacheTables = false;
    
    private Map<String,Object> properties;

    public DBUnitConfig() {
        this.executorId = DataSetExecutorImpl.DEFAULT_EXECUTOR_ID;
    }

    public DBUnitConfig(String executor) {
        properties = new HashMap<>();
        this.executorId = executor;
        if("".equals(this.executorId)){
            this.executorId = DataSetExecutorImpl.DEFAULT_EXECUTOR_ID;
        }
    } 
    
    
    public static DBUnitConfig from(DBUnit dbUnit){
        DBUnitConfig dbUnitConfig = new DBUnitConfig(dbUnit.executor());
        
        dbUnitConfig.cacheConnection(dbUnit.cacheConnection()).
            cacheTables(dbUnit.cacheTableNames()).
            addDBUnitProperty("batchedStatements", dbUnit.batchedStatements()).
            addDBUnitProperty("batchSize", dbUnit.batchSize()).
            addDBUnitProperty("allowEmptyFields", dbUnit.allowEmptyFields()).
            addDBUnitProperty("fetchSize", dbUnit.fetchSize()).
            addDBUnitProperty("qualifiedTableNames", dbUnit.qualifiedTableNames());

        if(!"".equals(dbUnit.escapePattern())){
            dbUnitConfig.addDBUnitProperty("escapePattern", dbUnit.escapePattern());
        }

        return dbUnitConfig;
    }
    
    
    public DBUnitConfig cacheConnection(boolean cacheConnection){
        this.cacheConnection = cacheConnection;
        return this;
    }
    
    
    public DBUnitConfig cacheTables(boolean cacheTables){
        this.cacheTables = cacheTables;
        return this;
    }
    
    public DBUnitConfig addDBUnitProperty(String name, Object value){
        properties.put(name, value);
        return this;
    }

    public void setCacheConnection(boolean cacheConnection) {
        this.cacheConnection = cacheConnection;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    public void setCacheTables(boolean cacheTables) {
        this.cacheTables = cacheTables;
    }

    public boolean isCacheConnection() {
        return cacheConnection;
    }


    public boolean isCacheTables() {
        return cacheTables;
    }


    public Map<String, Object> getProperties() {
        return properties;
    }
    
    public String getExecutorId() {
        return executorId;
    }


}
