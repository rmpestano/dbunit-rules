package com.github.dbunit.rules.dataset;

import com.github.dbunit.rules.type.SeedStrategy;

import java.lang.annotation.*;

/**
 * Created by rafael-pestano on 22/07/2015.
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface DataSet {

  /**
   * dataset file name using resources folder as root directory
   */
  String value();

  SeedStrategy strategy() default SeedStrategy.CLEAN_INSERT;

  /**
   * looks at the constraints and the dataset and determines the correct ordering for the SQL statements
   */
  boolean useSequenceFiltering() default false;


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