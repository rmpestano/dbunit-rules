package com.github.dbunit.rules.cdi.api;

import javax.interceptor.InterceptorBinding;
import java.lang.annotation.*;

/**
 * Created by pestano on 26/07/15.
 */
@Inherited
@InterceptorBinding
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
public @interface DataSetInterceptor {

}