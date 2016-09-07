package com.github.dbunit.junit5;

import com.github.dbunit.rules.api.connection.ConnectionHolder;
import com.github.dbunit.rules.api.dataset.DataSet;
import com.github.dbunit.rules.api.dataset.DataSetExecutor;
import com.github.dbunit.rules.api.dataset.ExpectedDataSet;
import com.github.dbunit.rules.configuration.DBUnitConfig;
import com.github.dbunit.rules.configuration.DataSetConfig;
import com.github.dbunit.rules.dataset.DataSetExecutorImpl;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestExtensionContext;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Optional;

import static com.github.dbunit.rules.util.EntityManagerProvider.em;
import static com.github.dbunit.rules.util.EntityManagerProvider.isEntityManagerActive;

/**
 * Created by pestano on 27/08/16.
 */
public class DBUnitExtension implements BeforeTestExecutionCallback, AfterTestExecutionCallback {


    @Override
    public void beforeTestExecution(TestExtensionContext testExtensionContext) throws Exception {

        if (!shouldCreateDataSet(testExtensionContext)) {
            return;
        }

        if (isEntityManagerActive()) {
            em().clear();
        }

        ConnectionHolder connectionHolder = findTestConnection(testExtensionContext);

        DataSet annotation = testExtensionContext.getTestMethod().get().getAnnotation(DataSet.class);
        if (annotation == null) {
            //try to infer from class level annotation
            annotation = testExtensionContext.getTestClass().get().getAnnotation(DataSet.class);
        }

        if (annotation == null) {
            throw new RuntimeException("Could not find DataSet annotation for test " + testExtensionContext.getTestMethod().get().getName());
        }

        final DataSetConfig dasetConfig = new DataSetConfig().from(annotation);
        DataSetExecutor executor = DataSetExecutorImpl.instance(dasetConfig.getExecutorId(), connectionHolder);
        executor.setDBUnitConfig(DBUnitConfig.from(testExtensionContext.getTestMethod().get()));


        ExtensionContext.Namespace namespace = getExecutorNamespace(testExtensionContext);//one executor per test class
        testExtensionContext.getStore(namespace).put("executor", executor);
        testExtensionContext.getStore(namespace).put("model", dasetConfig);


        try {
            executor.createDataSet(dasetConfig);
        } catch (final Exception e) {
            throw new RuntimeException(String.format("Could not create dataset for test method %s due to following error " + e.getMessage(), testExtensionContext.getTestMethod().get().getName()), e);
        }
        boolean isTransactional = dasetConfig.isTransactional() && isEntityManagerActive();
        if (isTransactional) {
            em().getTransaction().begin();
        }

    }


    private boolean shouldCreateDataSet(TestExtensionContext testExtensionContext) {
        return testExtensionContext.getTestMethod().get().isAnnotationPresent(DataSet.class) || testExtensionContext.getTestClass().get().isAnnotationPresent(DataSet.class);
    }

    private boolean shouldCompareDataSet(TestExtensionContext testExtensionContext) {
        return testExtensionContext.getTestMethod().get().isAnnotationPresent(ExpectedDataSet.class) || testExtensionContext.getTestClass().get().isAnnotationPresent(ExpectedDataSet.class);
    }


    @Override
    public void afterTestExecution(TestExtensionContext testExtensionContext) throws Exception {

        if (shouldCompareDataSet(testExtensionContext)) {
            ExpectedDataSet expectedDataSet = testExtensionContext.getTestMethod().get().getAnnotation(ExpectedDataSet.class);
            if (expectedDataSet == null) {
                //try to infer from class level annotation
                expectedDataSet = testExtensionContext.getTestClass().get().getAnnotation(ExpectedDataSet.class);
            }
            if (expectedDataSet != null) {
                ExtensionContext.Namespace namespace = getExecutorNamespace(testExtensionContext);//one executor per test class
                DataSetExecutor executor = testExtensionContext.getStore(namespace).get("executor", DataSetExecutor.class);
                DataSetConfig datasetConfig = testExtensionContext.getStore(namespace).get("model", DataSetConfig.class);
                boolean isTransactional = datasetConfig.isTransactional() && isEntityManagerActive();
                if (isTransactional) {
                    em().getTransaction().commit();
                }
                executor.compareCurrentDataSetWith(new DataSetConfig(expectedDataSet.value()).disableConstraints(true), expectedDataSet.ignoreCols());
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

}
