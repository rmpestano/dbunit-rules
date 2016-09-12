package com.github.dbunit.rules.cdi;

import com.github.dbunit.rules.api.dataset.DataSetExecutor;
import com.github.dbunit.rules.api.expoter.DataSetExportConfig;
import com.github.dbunit.rules.api.expoter.ExportDataSet;
import com.github.dbunit.rules.configuration.DBUnitConfig;
import com.github.dbunit.rules.configuration.DataSetConfig;
import com.github.dbunit.rules.connection.ConnectionHolderImpl;
import com.github.dbunit.rules.dataset.DataSetExecutorImpl;
import com.github.dbunit.rules.exporter.DataSetExporterImpl;
import org.dbunit.DatabaseUnitException;
import org.hibernate.Session;
import org.hibernate.internal.SessionImpl;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Level;

/**
 * Created by rafael-pestano on 08/10/2015.
 */
@RequestScoped
public class DataSetProcessor {

    public static final String CDI_DBUNIT_EXECUTOR = "CDI_DBUNIT_EXECUTOR";

    private static final Logger log = LoggerFactory.getLogger(DataSetProcessor.class.getName());

    @Inject
    private EntityManager em;

    private Connection connection;

    private DataSetExecutor dataSetExecutor;

    @PostConstruct
    public void init() {
        if (em == null) {
            throw new RuntimeException("Please provide an entity manager via CDI producer, see examples here: https://deltaspike.apache.org/documentation/jpa.html");
        }
        em.clear();
        this.connection = createConnection();
        dataSetExecutor = DataSetExecutorImpl.instance(CDI_DBUNIT_EXECUTOR, new ConnectionHolderImpl(connection));
    }

    /**
     * unfortunately there is no standard way to get jdbc connection from JPA entity manager
     *
     * @return JDBC connection
     */
    private Connection createConnection() {
        try {
            EntityTransaction tx = this.em.getTransaction();
            if (isHibernatePresentOnClasspath() && em.getDelegate() instanceof Session) {
                connection = ((SessionImpl) em.unwrap(Session.class)).connection();
            } else {
                /**
                 * see here:http://wiki.eclipse.org/EclipseLink/Examples/JPA/EMAPI#Getting_a_JDBC_Connection_from_an_EntityManager
                 */
                tx.begin();
                connection = em.unwrap(Connection.class);
                tx.commit();
            }
        } catch (Exception e) {
            throw new RuntimeException("Could not create database connection", e);
        }

        return connection;
    }

    public void process(DataSetConfig dataSetConfig, DBUnitConfig config) {
        dataSetExecutor.setDBUnitConfig(config);
        dataSetExecutor.createDataSet(dataSetConfig);
    }


    private boolean isHibernatePresentOnClasspath() {
        try {
            Class.forName("org.hibernate.Session");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    public void clearDatabase(DataSetConfig DataSetConfig) {
        try {
            dataSetExecutor.clearDatabase(DataSetConfig);
        } catch (SQLException e) {
            log.error("Could not clear database.", e);
        }
    }

    public void executeStatements(String[] executeStatementsAfter) {
        dataSetExecutor.executeStatements(executeStatementsAfter);
    }

    public void executeScript(String script) {
        dataSetExecutor.executeScript(script);
    }

    public void compareCurrentDataSetWith(DataSetConfig expected, String[] excludeCols) throws DatabaseUnitException {
        dataSetExecutor.compareCurrentDataSetWith(expected, excludeCols);
    }

    public Connection getConnection() {
        return connection;
    }

    public void exportDataSet(Method method) {
        ExportDataSet exportDataSet = resolveExportDataSet(method);
        if(exportDataSet != null){
            DataSetExportConfig exportConfig = DataSetExportConfig.from(exportDataSet);
            String outputName = exportConfig.getOutputFileName();
            if(outputName == null || "".equals(outputName.trim())){
                outputName = method.getName().toLowerCase()+"."+exportConfig.getDataSetFormat().name().toLowerCase();
            }
            try {
                DataSetExporterImpl.getInstance().export(dataSetExecutor.getDBUnitConnection(),exportConfig ,outputName);
            } catch (Exception e) {
                java.util.logging.Logger.getLogger(getClass().getName()).log(Level.WARNING,"Could not export dataset after method "+method.getName(),e);
            }
        }
    }

    private ExportDataSet resolveExportDataSet(Method method) {
        ExportDataSet exportDataSet = method.getAnnotation(ExportDataSet.class);
        if (exportDataSet == null) {
            exportDataSet = method.getDeclaringClass().getAnnotation(ExportDataSet.class);
        }
        return exportDataSet;
    }
}