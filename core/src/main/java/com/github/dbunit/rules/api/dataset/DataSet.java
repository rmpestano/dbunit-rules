package com.github.dbunit.rules.api.dataset;

import com.github.dbunit.rules.dataset.DataSetExecutorImpl;

import java.lang.annotation.*;

/**
 * Created by rafael-pestano on 22/07/2015.
 */
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface DataSet {

  /**
   * @return dataset file name using resources folder as root directory
   */
  String value() default "";

  /**
   *
   * @return name of dataset executor for the given dataset. If not specified the default one will be used.
   *
   * Use this option to work with multple database conncetions. Remember that each executor has its own connection.
   */
  String executorId() default DataSetExecutorImpl.DEFAULT_EXECUTOR_ID;

  SeedStrategy strategy() default SeedStrategy.CLEAN_INSERT;

  /**
   * @return a boolean looks at constraints and dataset and tries to determine the correct ordering for the SQL statements
   */
  boolean useSequenceFiltering() default true;

  /**
   * @return a list of table names used to reorder DELETE operations to prevent failures due to circular dependencies
   *
   */
  String[] tableOrdering() default {};


  boolean disableConstraints() default false;

  /**
   * @return a list of jdbc statements to execute before test
   *
   */
  String[] executeStatementsBefore() default {};

  /**
   * @return a list of jdbc statements to execute after test
   */
  String[] executeStatementsAfter() default {};

  /**
   * @return a list of sql script files to execute before test.
   * Note that commands inside sql file must be separated by ';'
   *
   */
  String[] executeScriptsBefore() default {};

  /**
   * @return a list of sql script files to execute after test.
   * Note that commands inside sql file must be separated by ';'
   */
  String[] executeScriptsAfter() default {};

  /**
   * @return if true DBUnit rules will try to delete database before test in a 'smart way'
   */
  boolean cleanBefore() default false;

  /**
   * @return if true DBUnit rules will try to delete database after test in a 'smart way'
   */
  boolean cleanAfter() default false;

}