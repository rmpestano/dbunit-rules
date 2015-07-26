package com.github.dbunit.rules.jpa;


import java.lang.annotation.*;

/**
 * Created by rafael-pestano on 22/07/2015.
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface Transactional {



}