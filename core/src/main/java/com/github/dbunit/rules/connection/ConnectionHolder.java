package com.github.dbunit.rules.connection;

import java.io.Serializable;
import java.sql.Connection;

/**
 * Created by pestano on 25/07/15.
 */
public interface ConnectionHolder extends Serializable{

    Connection getConnection();


}
