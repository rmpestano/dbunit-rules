package com.github.dbunit.rules.jpa;

import com.github.dbunit.rules.api.dataset.DataSetExecutor;
import com.github.dbunit.rules.api.connection.ConnectionHolder;
import com.github.dbunit.rules.connection.ConnectionHolderImpl;
import com.github.dbunit.rules.dataset.DataSetExecutorImpl;
import com.github.dbunit.rules.api.dataset.DataSetModel;
import org.hibernate.Session;
import org.hibernate.internal.SessionImpl;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by pestano on 28/07/15.
 */
public class JPADataSetExecutor implements DataSetExecutor {

    private EntityManager entityManager;
    private static Map<String, JPADataSetExecutor> executors = new HashMap<>();
    private ConnectionHolder connection;
    private DataSetExecutorImpl executor;
    private DataSetModel dataSetModel;


    public static JPADataSetExecutor instance(EntityManager entityManager) {
        return instance(null, entityManager);
    }

    public static JPADataSetExecutor instance(String executorId, EntityManager entityManager) {
        if (executorId == null) {
            executorId = DataSetExecutorImpl.DEFAULT_EXECUTOR_ID;
        }
        JPADataSetExecutor instance = executors.get(executorId);
        if (instance == null) {
            instance = new JPADataSetExecutor();
            executors.put(executorId, instance);
        }
        instance.setEntityManager(entityManager);
        if (instance.executor == null) {
            instance.executor = DataSetExecutorImpl.instance(executorId, instance.getConnectionHolder());

        }
        return instance;
    }

    public void setEntityManager(EntityManager entityManager) {
        this.entityManager = entityManager;
    }

    public EntityManager getEntityManager() {
        return entityManager;
    }

    public ConnectionHolder getConnectionHolder() {
        if (entityManager == null || !entityManager.isOpen()) {
            throw new RuntimeException("Could not get jdbc connection. Entity manager is null or is closed");
        }
        try {
            if (connection == null || connection.getConnection().isClosed()) {
                connection = new ConnectionHolderImpl(this.getJdbcConnection());
            }
        } catch (SQLException e) {
            LoggerFactory.getLogger(JPADataSetExecutor.class).error("Could not create JPA connection", e);
        }
        return connection;
    }

    public Connection getJdbcConnection() {
        if (entityManager.getDelegate() instanceof Session) {
            return ((SessionImpl) entityManager.getDelegate()).connection();
        } else {
            entityManager.getTransaction().begin();
            Connection connection = entityManager.unwrap(Connection.class);
            entityManager.getTransaction().commit();
            return connection;
        }
    }

    public void createDataSet(DataSetModel model) {
        executor.createDataSet(model);
    }

    public void createDataSet() {
        executor.createDataSet(dataSetModel);
    }


    @Override
    public DataSetModel getDataSetModel() {
        return dataSetModel;
    }

    @Override
    public void clearDatabase(DataSetModel dataset) throws SQLException {
        if(dataset == null){
            dataset = dataSetModel;
        }
        executor.clearDatabase(dataset);
    }

    @Override
    public void executeStatements(String[] statements) {
        executor.executeStatements(statements);

    }

    @Override
    public void executeScript(String scriptPath) {
        executor.executeScript(scriptPath);
    }

    @Override
    public void setDataSetModel(DataSetModel dataSetModel) {
        this.dataSetModel = dataSetModel;
    }
}
