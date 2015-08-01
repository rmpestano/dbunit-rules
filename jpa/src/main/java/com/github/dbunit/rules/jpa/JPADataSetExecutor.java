package com.github.dbunit.rules.jpa;

import com.github.dbunit.rules.connection.ConnectionHolder;
import com.github.dbunit.rules.connection.ConnectionHolderImpl;
import com.github.dbunit.rules.dataset.DataSetExecutor;
import com.github.dbunit.rules.dataset.DataSetModel;
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
public class JPADataSetExecutor {

    private EntityManager entityManager;
    private static Map<String,JPADataSetExecutor> executors = new HashMap<>();
    private ConnectionHolder connection;
    private DataSetExecutor executor;


    public static JPADataSetExecutor instance(EntityManager entityManager){
        return instance(null,entityManager);
    }
    public static JPADataSetExecutor instance(String executorId,EntityManager entityManager){
        if(executorId == null){
            executorId = DataSetExecutor.DEFAULT_EXECUTOR_ID;
        }
        JPADataSetExecutor instance = executors.get(executorId);
        if(instance == null){
            instance = new JPADataSetExecutor();
        }
        instance.setEntityManager(entityManager);
        if(instance.executor == null){
            try {
                instance.executor = DataSetExecutor.instance(executorId,instance.getConnectionHolder());
            } catch (SQLException e) {
                LoggerFactory.getLogger(JPADataSetExecutor.class).error("Could not create JPA connection", e);
            }
        }
        return instance;
    }

    public void setEntityManager(EntityManager entityManager) {
        this.entityManager = entityManager;
    }

    public EntityManager getEntityManager() {
        return entityManager;
    }

    public ConnectionHolder getConnectionHolder() throws SQLException {
        if(entityManager == null || !entityManager.isOpen()){
            throw new RuntimeException("Could not get jdbc connection. Entity manager is null or is closed");
        }
        if(connection == null || connection.getConnection().isClosed()){
            connection = new ConnectionHolderImpl(this.getJdbcConnection());
        }
        return connection;
    }

    public Connection getJdbcConnection() {
        if (entityManager.getDelegate() instanceof Session){
            return ((SessionImpl)entityManager.getDelegate()).connection();
        }else{
            entityManager.getTransaction().begin();
            Connection connection = entityManager.unwrap(Connection.class);
            entityManager.getTransaction().commit();
            return connection;
        }
    }

    public void execute(DataSetModel model){
        executor.execute(model);
    }
}
