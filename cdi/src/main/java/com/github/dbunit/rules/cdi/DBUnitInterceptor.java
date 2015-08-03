package com.github.dbunit.rules.cdi;

import com.github.dbunit.rules.api.connection.ConnectionHolder;
import com.github.dbunit.rules.cdi.api.DataSetInterceptor;
import com.github.dbunit.rules.cdi.api.JPADataSet;
import com.github.dbunit.rules.connection.ConnectionHolderImpl;
import com.github.dbunit.rules.dataset.DataSetExecutorImpl;
import com.github.dbunit.rules.api.dataset.DataSetModel;
import com.github.dbunit.rules.jpa.EntityManagerProvider;
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


    @AroundInvoke
    public Object intercept(InvocationContext invocationContext)
            throws Exception {

        if (invocationContext.getMethod().getAnnotation(JPADataSet.class) != null) {
            JPADataSet dataSet = invocationContext.getMethod().getAnnotation(JPADataSet.class);
            if (dataSet != null) {
                if (dataSet.unitName() == null || "".equals(dataSet.unitName())) {
                    throw new RuntimeException("Provide JPA unit unit name in order to use DBUnitInterceptor");
                }
                Connection connection = null;
                EntityManager entityManager = EntityManagerProvider.instance(dataSet.unitName()).em();
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
                    throw new RuntimeException("Could not get jdbc connection from provided unit name "+dataSet.unitName());
                }
                ConnectionHolder connectionHolder = new ConnectionHolderImpl(connection);
                DataSetModel dataSetModel = new DataSetModel(dataSet.value()).disableConstraints(dataSet.disableConstraints()).
                        executeStatementsAfter(dataSet.executeStatementsAfter()).executeStatementsBefore(dataSet.executeStatementsBefore()).
                        seedStrategy(dataSet.strategy()).tableOrdering(dataSet.tableOrdering()).
                        useSequenceFiltering(dataSet.useSequenceFiltering());
                //one executor per class
                DataSetExecutorImpl.instance(invocationContext.getMethod().getDeclaringClass().getSimpleName(), connectionHolder).createDataSet(dataSetModel);
            }
        }

        return invocationContext.proceed();
    }


}
