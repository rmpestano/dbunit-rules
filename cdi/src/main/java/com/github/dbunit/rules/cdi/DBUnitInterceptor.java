package com.github.dbunit.rules.cdi;

import com.github.dbunit.rules.api.connection.ConnectionHolder;
import com.github.dbunit.rules.cdi.api.DataSetInterceptor;
import com.github.dbunit.rules.connection.ConnectionHolderImpl;
import com.github.dbunit.rules.api.dataset.DataSet;
import com.github.dbunit.rules.dataset.DataSetExecutorImpl;
import com.github.dbunit.rules.api.dataset.DataSetModel;
import org.hibernate.Session;
import org.hibernate.internal.SessionImpl;

import javax.inject.Inject;
import javax.interceptor.AroundInvoke;
import javax.interceptor.Interceptor;
import javax.interceptor.InvocationContext;
import javax.persistence.EntityManager;
import java.io.Serializable;
import java.sql.Connection;

/**
 * Created by pestano on 26/07/15.
 */
@Interceptor
@DataSetInterceptor
public class DBUnitInterceptor implements Serializable {

    @Inject
    EntityManager entityManager;

    @AroundInvoke
    public Object intercept(InvocationContext invocationContext)
            throws Exception {

        if (invocationContext.getMethod().getAnnotation(DataSet.class) != null) {
            DataSet dataSet = invocationContext.getMethod().getAnnotation(DataSet.class);
            if (dataSet != null) {
                if (entityManager == null) {
                    throw new RuntimeException("Provide entityManager instance via CDI @Producer in order to use DBUnitInterceptor");
                }
                Connection connection = null;
                if (entityManager.getDelegate() instanceof Session) {
                    connection = ((SessionImpl) entityManager.unwrap(Session.class)).connection();
                } else {
                    /**
                     * see here:http://wiki.eclipse.org/EclipseLink/Examples/JPA/EMAPI#Getting_a_JDBC_Connection_from_an_EntityManager
                     */
                    entityManager.getTransaction().begin();
                    connection = entityManager.unwrap(java.sql.Connection.class);
                    entityManager.getTransaction().commit();
                }

                if (connection == null) {
                    throw new RuntimeException("Could not get jdbc connection from provided entityManager");
                }
                ConnectionHolder connectionHolder = new ConnectionHolderImpl(connection);
                DataSetModel dataSetModel = new DataSetModel().from(dataSet);
                //one executor per class
                DataSetExecutorImpl.instance(invocationContext.getMethod().getDeclaringClass().getSimpleName(), connectionHolder).createDataSet(dataSetModel);
            }
        }

        return invocationContext.proceed();
    }


}
