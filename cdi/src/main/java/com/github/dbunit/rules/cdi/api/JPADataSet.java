package com.github.dbunit.rules.cdi.api;

import com.github.dbunit.rules.api.dataset.SeedStrategy;
import com.github.dbunit.rules.dataset.DataSetExecutorImpl;

import javax.enterprise.util.Nonbinding;
import javax.inject.Qualifier;
import java.lang.annotation.*;

/**
 * Created by pestano on 01/08/15.
 */
@Inherited
@Qualifier
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE,ElementType.PARAMETER,ElementType.FIELD})
public @interface JPADataSet {

    @Nonbinding
    String unitName() default "";


    /**
     * @return dataset file name using resources folder as root directory
     */
    @Nonbinding
    String value() default "";


    @Nonbinding
    SeedStrategy strategy() default SeedStrategy.CLEAN_INSERT;

    /**
     * @return a boolean looks at constraints and dataset and tries to determine the correct ordering for the SQL statements
     */
    @Nonbinding
    boolean useSequenceFiltering() default true;

    /**
     * @return a list of table names used to reorder DELETE operations to prevent failures due to circular dependencies
     *
     */
    @Nonbinding
    String[] tableOrdering() default {};


    @Nonbinding
    boolean disableConstraints() default false;

    /**
     * @return a list of jdbc statements to createDataSet before test
     *
     */
    @Nonbinding
    String[] executeStatementsBefore() default {};

    /**
     * @return a list of jdbc statements to createDataSet after test
     */
    @Nonbinding
    String[] executeStatementsAfter() default {};

}
