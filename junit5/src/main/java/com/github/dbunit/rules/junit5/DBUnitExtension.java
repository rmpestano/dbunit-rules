package com.github.dbunit.rules.junit5;

import com.github.dbunit.rules.api.connection.ConnectionHolder;
import com.github.dbunit.rules.api.dataset.DataSet;
import com.github.dbunit.rules.api.dataset.DataSetExecutor;
import com.github.dbunit.rules.api.dataset.ExpectedDataSet;
import com.github.dbunit.rules.api.expoter.DataSetExportConfig;
import com.github.dbunit.rules.api.expoter.ExportDataSet;
import com.github.dbunit.rules.api.leak.LeakHunter;
import com.github.dbunit.rules.configuration.ConnectionConfig;
import com.github.dbunit.rules.configuration.DBUnitConfig;
import com.github.dbunit.rules.configuration.DataSetConfig;
import com.github.dbunit.rules.connection.ConnectionHolderImpl;
import com.github.dbunit.rules.dataset.DataSetExecutorImpl;
import com.github.dbunit.rules.exporter.DataSetExporter;
import com.github.dbunit.rules.leak.LeakHunterException;
import com.github.dbunit.rules.leak.LeakHunterFactory;
import org.dbunit.DatabaseUnitException;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestExtensionContext;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.github.dbunit.rules.util.EntityManagerProvider.em;
import static com.github.dbunit.rules.util.EntityManagerProvider.isEntityManagerActive;

/**
 * Created by pestano on 27/08/16.
 */
public class DBUnitExtension implements BeforeTestExecutionCallback, AfterTestExecutionCallback {

    private static final String EXECUTOR_STORE = "executor";
    private static final String DATASET_CONFIG_STORE = "datasetConfig";
    private static final String LEAK_STORE = "leakHunter";
    private static final String CONNECTION_BEFORE_STORE = "openConnectionsBefore";

    @Override
    public void beforeTestExecution(TestExtensionContext testExtensionContext) throws Exception {

        if (!shouldCreateDataSet(testExtensionContext)) {
            return;
        }

        ConnectionHolder connectionHolder = findTestConnection(testExtensionContext);

        if (isEntityManagerActive()) {
            em().clear();
        }


        DataSet annotation = testExtensionContext.getTestMethod().get().getAnnotation(DataSet.class);
        if (annotation == null) {
            //try to infer from class level annotation
            annotation = testExtensionContext.getTestClass().get().getAnnotation(DataSet.class);
        }


        DBUnitConfig dbUnitConfig = DBUnitConfig.from(testExtensionContext.getTestMethod().get());
        final DataSetConfig dataSetConfig = new DataSetConfig().from(annotation);
        if(connectionHolder == null || connectionHolder.getConnection() == null){
            connectionHolder = createConnection(dbUnitConfig,testExtensionContext.getTestMethod().get().getName());
        }
        DataSetExecutor executor = DataSetExecutorImpl.instance(dataSetConfig.getExecutorId(), connectionHolder);
        executor.setDBUnitConfig(dbUnitConfig);



        if (dataSetConfig != null && dataSetConfig.getExecuteStatementsBefore() != null && dataSetConfig.getExecuteStatementsBefore().length > 0) {
            try {
                executor.executeStatements(dataSetConfig.getExecuteStatementsBefore());
            } catch (Exception e) {
                LoggerFactory.getLogger(getClass().getName()).error(testExtensionContext.getTestMethod().get().getName() + "() - Could not execute statements Before:" + e.getMessage(), e);
            }
        }//end execute statements

        if (dataSetConfig.getExecuteScriptsBefore() != null && dataSetConfig.getExecuteScriptsBefore().length > 0) {
            try {
                for (int i = 0; i < dataSetConfig.getExecuteScriptsBefore().length; i++) {
                    executor.executeScript(dataSetConfig.getExecuteScriptsBefore()[i]);
                }
            } catch (Exception e) {
                if (e instanceof DatabaseUnitException) {
                    throw e;
                }
                LoggerFactory.getLogger(getClass().getName()).error(testExtensionContext.getTestMethod().get().getName() + "() - Could not execute scriptsBefore:" + e.getMessage(), e);
            }
        }//end execute scripts
        
        ExtensionContext.Namespace namespace = getExecutorNamespace(testExtensionContext);//one executor per test class
        testExtensionContext.getStore(namespace).put(EXECUTOR_STORE, executor);
        testExtensionContext.getStore(namespace).put(DATASET_CONFIG_STORE, dataSetConfig);
        if (dbUnitConfig.isLeakHunter()) {
            LeakHunter leakHunter = LeakHunterFactory.from(connectionHolder.getConnection());
            testExtensionContext.getStore(namespace).put(LEAK_STORE, leakHunter);
            testExtensionContext.getStore(namespace).put(CONNECTION_BEFORE_STORE, leakHunter.openConnections());
        }

        try {
            executor.createDataSet(dataSetConfig);
        } catch (final Exception e) {
            throw new RuntimeException(String.format("Could not create dataset for test method %s due to following error " + e.getMessage(), testExtensionContext.getTestMethod().get().getName()), e);
        }

        boolean isTransactional = dataSetConfig.isTransactional();
        if (isTransactional) {
            if (isEntityManagerActive()) {
                em().getTransaction().begin();
            } else{
                Connection connection = executor.getConnectionHolder().getConnection();
                connection.setAutoCommit(false);
            }
        }

    }


    private boolean shouldCreateDataSet(TestExtensionContext testExtensionContext) {
        return testExtensionContext.getTestMethod().get().isAnnotationPresent(DataSet.class) || testExtensionContext.getTestClass().get().isAnnotationPresent(DataSet.class);
    }

    private boolean shouldCompareDataSet(TestExtensionContext testExtensionContext) {
        return testExtensionContext.getTestMethod().get().isAnnotationPresent(ExpectedDataSet.class) || testExtensionContext.getTestClass().get().isAnnotationPresent(ExpectedDataSet.class);
    }

    private boolean shouldExportDataSet(TestExtensionContext testExtensionContext) {
        return testExtensionContext.getTestMethod().get().isAnnotationPresent(ExportDataSet.class) || testExtensionContext.getTestClass().get().isAnnotationPresent(ExportDataSet.class);
    }

    public void exportDataSet(DataSetExecutor dataSetExecutor, Method method) {
        ExportDataSet exportDataSet = resolveExportDataSet(method);
        if(exportDataSet != null){
            DataSetExportConfig exportConfig = DataSetExportConfig.from(exportDataSet);
            String outputName = exportConfig.getOutputFileName();
            if(outputName == null || "".equals(outputName.trim())){
                outputName = method.getName().toLowerCase()+"."+exportConfig.getDataSetFormat().name().toLowerCase();
            }
            exportConfig.outputFileName(outputName);
            try {
                DataSetExporter.getInstance().export(dataSetExecutor.getDBUnitConnection(),exportConfig);
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


    @Override
    public void afterTestExecution(TestExtensionContext testExtensionContext) throws Exception {
        DBUnitConfig dbUnitConfig = DBUnitConfig.from(testExtensionContext.getTestMethod().get());
        ExtensionContext.Namespace executorNamespace = getExecutorNamespace(testExtensionContext);
        try {
            if (shouldCompareDataSet(testExtensionContext)) {
                ExpectedDataSet expectedDataSet = testExtensionContext.getTestMethod().get().getAnnotation(ExpectedDataSet.class);
                if (expectedDataSet == null) {
                    //try to infer from class level annotation
                    expectedDataSet = testExtensionContext.getTestClass().get().getAnnotation(ExpectedDataSet.class);
                }
                if (expectedDataSet != null) {
                    ExtensionContext.Namespace namespace = getExecutorNamespace(testExtensionContext);//one executor per test class
                    DataSetExecutor executor = testExtensionContext.getStore(namespace).get(EXECUTOR_STORE, DataSetExecutor.class);
                    DataSetConfig datasetConfig = testExtensionContext.getStore(namespace).get(DATASET_CONFIG_STORE, DataSetConfig.class);
                    boolean isTransactional = datasetConfig.isTransactional();
                    if (isTransactional) {
                        if(isEntityManagerActive()){
                            em().getTransaction().commit();
                        } else{
                            Connection connection = executor.getConnectionHolder().getConnection();
                            connection.commit();
                            connection.setAutoCommit(false);
                        }
                    }
                    executor.compareCurrentDataSetWith(new DataSetConfig(expectedDataSet.value()).disableConstraints(true), expectedDataSet.ignoreCols());
                }
            }

            if (dbUnitConfig != null && dbUnitConfig.isLeakHunter()) {
                LeakHunter leakHunter = testExtensionContext.getStore(executorNamespace).get(LEAK_STORE, LeakHunter.class);
                int openConnectionsBefore = testExtensionContext.getStore(executorNamespace).get(CONNECTION_BEFORE_STORE, Integer.class);
                int openConnectionsAfter = leakHunter.openConnections();
                if (openConnectionsAfter > openConnectionsBefore) {
                    throw new LeakHunterException(testExtensionContext.getTestMethod().get().getName(), openConnectionsAfter - openConnectionsBefore);
                }

            }

        } finally {

            DataSetConfig dataSetConfig = testExtensionContext.getStore(executorNamespace).get(DATASET_CONFIG_STORE, DataSetConfig.class);
            if (dataSetConfig == null) {
                return;
            }
            DataSetExecutor executor = testExtensionContext.getStore(executorNamespace).get(EXECUTOR_STORE, DataSetExecutor.class);

            if(shouldExportDataSet(testExtensionContext)){
                exportDataSet(executor,testExtensionContext.getTestMethod().get());
            }

            if (dataSetConfig != null && dataSetConfig.getExecuteStatementsAfter() != null && dataSetConfig.getExecuteStatementsAfter().length > 0) {
                try {
                    executor.executeStatements(dataSetConfig.getExecuteStatementsAfter());
                } catch (Exception e) {
                    LoggerFactory.getLogger(getClass().getName()).error(testExtensionContext.getTestMethod().get().getName() + "() - Could not execute statements after:" + e.getMessage(), e);
                }
            }//end execute statements

            if (dataSetConfig.getExecuteScriptsAfter() != null && dataSetConfig.getExecuteScriptsAfter().length > 0) {
                try {
                    for (int i = 0; i < dataSetConfig.getExecuteScriptsAfter().length; i++) {
                        executor.executeScript(dataSetConfig.getExecuteScriptsAfter()[i]);
                    }
                } catch (Exception e) {
                    if (e instanceof DatabaseUnitException) {
                        throw e;
                    }
                    LoggerFactory.getLogger(getClass().getName()).error(testExtensionContext.getTestMethod().get().getName() + "() - Could not execute scriptsAfter:" + e.getMessage(), e);
                }
            }//end execute scripts

            if (dataSetConfig.isCleanAfter()) {
                executor.clearDatabase(dataSetConfig);
            }
        }

    }

    private ExtensionContext.Namespace getExecutorNamespace(TestExtensionContext testExtensionContext) {
        return ExtensionContext.Namespace.create("DBUnitExtension-" + testExtensionContext.getTestClass().get());//one executor per test class
    }


    private ConnectionHolder findTestConnection(TestExtensionContext testExtensionContext) {
        Class<?> testClass = testExtensionContext.getTestClass().get();
        try {
            Optional<Field> fieldFound = Arrays.stream(testClass.getDeclaredFields()).
                    filter(f -> f.getType() == ConnectionHolder.class).
                    findFirst();

            if (fieldFound.isPresent()) {
                Field field = fieldFound.get();
                if (!field.isAccessible()) {
                    field.setAccessible(true);
                }
                ConnectionHolder connectionHolder = ConnectionHolder.class.cast(field.get(testExtensionContext.getTestInstance()));
                if (connectionHolder == null || connectionHolder.getConnection() == null) {
                    throw new RuntimeException("ConnectionHolder not initialized correctly");
                }
                return connectionHolder;
            }

            //try to get connection from method

            Optional<Method> methodFound = Arrays.stream(testClass.getDeclaredMethods()).
                    filter(m -> m.getReturnType() == ConnectionHolder.class).
                    findFirst();

            if (methodFound.isPresent()) {
                Method method = methodFound.get();
                if (!method.isAccessible()) {
                    method.setAccessible(true);
                }
                ConnectionHolder connectionHolder = ConnectionHolder.class.cast(method.invoke(testExtensionContext.getTestInstance()));
                if (connectionHolder == null || connectionHolder.getConnection() == null) {
                    throw new RuntimeException("ConnectionHolder not initialized correctly");
                }
                return connectionHolder;
            }

        } catch (Exception e) {
            throw new RuntimeException("Could not get database connection for test " + testClass, e);
        }

        return null;


    }


    private ConnectionHolder createConnection(DBUnitConfig dbUnitConfig, String currentMethod) {
        ConnectionConfig connectionConfig = dbUnitConfig.getConnectionConfig();
        if ("".equals(connectionConfig.getUrl()) || "".equals(connectionConfig.getUser())) {
            throw new RuntimeException(String.format("Could not create JDBC connection for method %s, provide a connection at test level or via configuration, see documentation here: https://github.com/rmpestano/dbunit-rules#jdbc-connection", currentMethod));
        }

        try {
            if (!"".equals(connectionConfig.getDriver())) {
                Class.forName(connectionConfig.getDriver());
            }
            return new ConnectionHolderImpl(DriverManager.getConnection(connectionConfig.getUrl(), connectionConfig.getUser(), connectionConfig.getPassword()));
        } catch (Exception e) {
            Logger.getLogger(getClass().getName()).log(Level.SEVERE, "Could not create JDBC connection for method " + currentMethod, e);
        }
        return null;
    }

}
