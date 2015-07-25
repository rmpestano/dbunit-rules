package com.github.dbunit.rules.dataset;

import com.github.dbunit.rules.type.SeedStrategy;

import java.lang.annotation.*;

/**
 * Created by rafael-pestano on 22/07/2015.
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface DataSet {

  /**
   * dataset file name using resources folder as root directory
   */
  String value();

  SeedStrategy strategy() default SeedStrategy.CLEAN_INSERT;

  /**
   * looks at constraints and dataset and tries to determine the correct ordering for the SQL statements
   */
  boolean useSequenceFiltering() default true;

  /**
   * can be used to reorder DELETE operations to prevent failures due to circular dependencies
   *
   */
  String[] tableOrdering() default {};


  boolean disableConstraints() default false;

  /**
   * a list of jdbc statements to execute before test
   */
  String[] executeStatementsBefore() default {};

  /**
   * a list of jdbc statements to execute after test
   */
  String[] executeStatementsAfter() default {};

}