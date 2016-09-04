package com.github.dbunit.rules.api.configuration;

import com.github.dbunit.rules.dataset.DataSetExecutorImpl;

import java.lang.annotation.*;

import org.dbunit.database.DatabaseConfig;

/**
 * Created by rafael-pestano on 30/08/2016.
 * 
 * This annotation configures DBUnit properties
 * (http://dbunit.sourceforge.net/properties.html) for a given dataset executor.
 * 
 * It can be used at class or method level.
 */
@Target({ ElementType.METHOD, ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface DBUnit {

    /**
     * 
     * @return executor id for which the properties will be setup.
     */
    String executor() default DataSetExecutorImpl.DEFAULT_EXECUTOR_ID;
    
    
    boolean cacheConnection() default true;
    
    
    boolean cacheTableNames() default true;
    

    /**
     * @return value which configures DatabaseConfig.FEATURE_QUALIFIED_TABLE_NAMES. Defaults to false.
     */
    boolean qualifiedTableNames() default false;
    
    /**
     * 
     * @return value which configures DatabaseConfig.FEATURE_BATCHED_STATEMENTS
     */
    boolean batchedStatements() default false;

    /**
     * @return value which configures DatabaseConfig.FEATURE_ALLOW_EMPTY_FIELDS. Defaults to false.
     *
     */
    boolean allowEmptyFields() default false;  
    
    /**
     * 
     * @return value which configures DatabaseConfig.PROPERTY_FETCH_SIZE. Defaults to 100
     */
    int fetchSize() default 100;
    
    /**
     * @return value which configures DatabaseConfig.PROPERTY_BATCH_SIZE. Defaults to 100
     */
    int batchSize() default 100;
    
    /**
     * @return value which configures DatabaseConfig.PROPERTY_ESCAPE_PATTERN. Defaults to none
     */
    String escapePattern() default "";
}