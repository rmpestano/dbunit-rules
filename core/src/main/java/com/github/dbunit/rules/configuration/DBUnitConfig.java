package com.github.dbunit.rules.configuration;

import com.github.dbunit.rules.api.configuration.DBUnit;
import com.github.dbunit.rules.dataset.DataSetExecutorImpl;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * represents DBUnit configuration of a dataset executor.
 */
public class DBUnitConfig {

    private String executorId;

    private boolean cacheConnection = false;

    private boolean cacheTableNames = false;

    private boolean leakHunter = false;

    private Map<String, Object> properties;

    public DBUnitConfig() {
        this.executorId = DataSetExecutorImpl.DEFAULT_EXECUTOR_ID;
    }

    public DBUnitConfig(String executor) {
        properties = new HashMap<>();
        this.executorId = executor;
        if ("".equals(this.executorId)) {
            this.executorId = DataSetExecutorImpl.DEFAULT_EXECUTOR_ID;
        }
    }


    public static DBUnitConfig from(DBUnit dbUnit) {
        DBUnitConfig dbUnitConfig = new DBUnitConfig(dbUnit.executor());

        dbUnitConfig.cacheConnection(dbUnit.cacheConnection()).
                cacheTableNames(dbUnit.cacheTableNames()).
                leakHunter(dbUnit.leakHunter()).
                addDBUnitProperty("batchedStatements", dbUnit.batchedStatements()).
                addDBUnitProperty("batchSize", dbUnit.batchSize()).
                addDBUnitProperty("allowEmptyFields", dbUnit.allowEmptyFields()).
                addDBUnitProperty("fetchSize", dbUnit.fetchSize()).
                addDBUnitProperty("qualifiedTableNames", dbUnit.qualifiedTableNames());

        if (!"".equals(dbUnit.escapePattern())) {
            dbUnitConfig.addDBUnitProperty("escapePattern", dbUnit.escapePattern());
        }

        return dbUnitConfig;
    }

    public static DBUnitConfig fromGlobalConfig() {
        return GlobalConfig.instance().getDbUnitConfig();
    }

    public static DBUnitConfig from(Method method) {
        DBUnit dbUnitConfig = method.getAnnotation(DBUnit.class);
        if (dbUnitConfig == null) {
            dbUnitConfig = method.getDeclaringClass().getAnnotation(DBUnit.class);
        }
        if (dbUnitConfig != null) {
            return from(dbUnitConfig);
        } else {
            return fromGlobalConfig();
        }
    }


    public DBUnitConfig cacheConnection(boolean cacheConnection) {
        this.cacheConnection = cacheConnection;
        return this;
    }

    public DBUnitConfig executorId(String executorId){
        this.executorId = executorId;
        return this;
    }

    public DBUnitConfig leakHunter(boolean leakHunter){
        this.leakHunter = leakHunter;
        return this;
    }


    public DBUnitConfig cacheTableNames(boolean cacheTables) {
        this.cacheTableNames = cacheTables;
        return this;
    }

    public DBUnitConfig addDBUnitProperty(String name, Object value) {
        properties.put(name, value);
        return this;
    }

    public void setCacheConnection(boolean cacheConnection) {
        this.cacheConnection = cacheConnection;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    public void setCacheTableNames(boolean cacheTableNames) {
        this.cacheTableNames = cacheTableNames;
    }

    public boolean isCacheConnection() {
        return cacheConnection;
    }


    public boolean isCacheTableNames() {
        return cacheTableNames;
    }

    public boolean isLeakHunter() {
        return leakHunter;
    }

    public void setLeakHunter(boolean activateLeakHunter) {
        this.leakHunter = activateLeakHunter;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public String getExecutorId() {
        return executorId;
    }


}
