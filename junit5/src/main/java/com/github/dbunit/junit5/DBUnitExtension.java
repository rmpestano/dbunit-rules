package com.github.dbunit.junit5;

import com.github.dbunit.rules.api.connection.ConnectionHolder;
import com.github.dbunit.rules.api.dataset.DataSet;
import com.github.dbunit.rules.api.dataset.DataSetExecutor;
import com.github.dbunit.rules.api.dataset.DataSetModel;
import com.github.dbunit.rules.api.dataset.ExpectedDataSet;
import com.github.dbunit.rules.dataset.DataSetExecutorImpl;
import org.junit.jupiter.api.extension.*;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

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

        if(isEntityManagerActive()){
            em().clear();
        }

        ConnectionHolder connectionHolder = findTestConnection(testExtensionContext);

        DataSet annotation = testExtensionContext.getTestMethod().get().getAnnotation(DataSet.class);
        if(annotation == null){
            //try to infer from class level annotation
            annotation = testExtensionContext.getTestClass().get().getAnnotation(DataSet.class);
        }

        if(annotation == null){
            throw new RuntimeException("Could not find DataSet annotation for test "+testExtensionContext.getTestMethod().get().getName());
        }

        final DataSetModel model = new DataSetModel().from(annotation);
        DataSetExecutor executor = DataSetExecutorImpl.instance(model.getExecutorId(), connectionHolder);

        ExtensionContext.Namespace namespace = getExecutorNamespace(testExtensionContext);//one executor per test class
        testExtensionContext.getStore(namespace).put("executor",executor);
        testExtensionContext.getStore(namespace).put("model",model);
        try {
            executor.createDataSet(model);
        } catch (final Exception e) {
            throw new RuntimeException(String.format("Could not create dataset for test method %s due to following error " + e.getMessage(), testExtensionContext.getTestMethod().get().getName()), e);
        }
        boolean isTransactional = model.isTransactional() && isEntityManagerActive();
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

        if(shouldCompareDataSet(testExtensionContext)){
            ExpectedDataSet expectedDataSet = testExtensionContext.getTestMethod().get().getAnnotation(ExpectedDataSet.class);
            if (expectedDataSet == null) {
                //try to infer from class level annotation
                expectedDataSet = testExtensionContext.getTestClass().get().getAnnotation(ExpectedDataSet.class);
            }
            if (expectedDataSet != null) {
                ExtensionContext.Namespace namespace = getExecutorNamespace(testExtensionContext);//one executor per test class
                DataSetExecutor executor = testExtensionContext.getStore(namespace).get("executor", DataSetExecutor.class);
                DataSetModel model = testExtensionContext.getStore(namespace).get("model", DataSetModel.class);
                boolean isTransactional = model.isTransactional() && isEntityManagerActive();
                if (isTransactional) {
                    em().getTransaction().commit();
                }
                executor.compareCurrentDataSetWith(new DataSetModel(expectedDataSet.value()).disableConstraints(true), expectedDataSet.ignoreCols());
            }
        }

    }

    private ExtensionContext.Namespace getExecutorNamespace(TestExtensionContext testExtensionContext) {
        return ExtensionContext.Namespace.create("DBUnitExtension-" + testExtensionContext.getTestClass().get());//one executor per test class
    }



    private ConnectionHolder findTestConnection(TestExtensionContext testExtensionContext) {
        Class<?> testClass = testExtensionContext.getTestClass().get();
        try {
            for (Field field : testClass.getDeclaredFields()) {
                if (field.getType() == ConnectionHolder.class) {
                    if (!field.isAccessible()) {
                        field.setAccessible(true);
                    }
                    ConnectionHolder connectionHolder = ConnectionHolder.class.cast(field.get(testExtensionContext.getTestInstance()));
                    if (connectionHolder == null || connectionHolder.getConnection() == null) {
                        throw new RuntimeException("ConnectionHolder not initialized correctly");
                    }
                    return connectionHolder;
                }
            }

            for (Method method : testClass.getDeclaredMethods()) {
                if (method.getReturnType() == ConnectionHolder.class) {
                    if (!method.isAccessible()) {
                        method.setAccessible(true);
                    }
                    ConnectionHolder connectionHolder = ConnectionHolder.class.cast(method.invoke(testExtensionContext.getTestInstance()));
                    if (connectionHolder == null || connectionHolder.getConnection() == null) {
                        throw new RuntimeException("ConnectionHolder not initialized correctly");
                    }
                    return connectionHolder;
                }
            }


        } catch (Exception e) {
            throw new RuntimeException("Could not get database connection for test " + testClass, e);
        }

        return null;


    }

}
