package com.github.dbunit.rules.cdi.api;

import java.lang.annotation.*;

import javax.enterprise.util.Nonbinding;
import javax.interceptor.InterceptorBinding;

import org.dbunit.operation.DatabaseOperation;

/**
 * Created by rafael-pestano on 22/07/2015.
 */
@InterceptorBinding
@Target({ ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface UsingDataSet {

  @Nonbinding
  String value() default "";

  @Nonbinding
  SeedStrategy seedStrategy() default SeedStrategy.CLEAN_INSERT;

  /**
   * @return if true dataset processor will look at constraints and dataset and try to determine the correct ordering for the SQL statements
   */
  @Nonbinding
  boolean useSequenceFiltering() default true;

  @Nonbinding
  boolean disableConstraints() default false;

  /**
   * clear database before dataset creation
   * @return
   */
  @Nonbinding
  boolean cleanAfter() default false;

  /**
   * clear database after test execution
   * @return
   */
  @Nonbinding
  boolean cleanBefore() default false;

  /**
   * @return a list of table names used to reorder DELETE operations to prevent failures due to circular dependencies
   *
   */
  @Nonbinding
  String[] tableOrdering() default {};

  /**
   *
   * @return list of sql commands to execute before dataset creation
   */
  @Nonbinding
  String[] executeCommandsBefore() default "";

  /**
   *
   * @return list of sql commands to execute after dataset creation
   */
  @Nonbinding
  String[] executeCommandsAfter() default "";


  enum SeedStrategy {
    CLEAN_INSERT(DatabaseOperation.CLEAN_INSERT), UPDATE(DatabaseOperation.UPDATE), INSERT(DatabaseOperation.INSERT), DELETE(DatabaseOperation.DELETE),
    REFRESH(DatabaseOperation.REFRESH), DELETE_ALL(DatabaseOperation.DELETE_ALL), TRUNCATE_TABLE(DatabaseOperation.TRUNCATE_TABLE);

    private final DatabaseOperation value;

    SeedStrategy(DatabaseOperation databaseOperation) {
      this.value = databaseOperation;
    }

    public DatabaseOperation getValue() {
      return value;
    }
  }

}