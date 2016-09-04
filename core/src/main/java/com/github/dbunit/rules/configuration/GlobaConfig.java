package com.github.dbunit.rules.configuration;

import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;

/**
 * Created by pestano on 03/09/16.
 * <p/>
 * pojo which represents dbunit.yml, used for global which can be overrided via @DataSet annotation
 * at class or method level and with @DBUnit at class or method level
 */
public class GlobaConfig {

    private static GlobaConfig instance;

    private DBUnitConfig dbUnitConfig;


    private GlobaConfig() {
    }

    public static GlobaConfig instance() {
        if (instance == null) {
            createInstance();
        }
        return instance;
    }

    public static GlobaConfig newInstance() {
        instance = null;
        return instance();
    }

    private static void createInstance() {
        instance = new GlobaConfig();
        DBUnitConfig dbUnitConfig;
        //try to instance user provided dbunit.yml
        InputStream customConfiguration = Thread.currentThread().getContextClassLoader().getResourceAsStream("dbunit.yml");
        if (customConfiguration != null) {
            dbUnitConfig = new Yaml().loadAs(customConfiguration, DBUnitConfig.class);
        } else {
            //default config
            dbUnitConfig = new Yaml().loadAs(GlobaConfig.class.getResourceAsStream("/default/dbunit.yml"), DBUnitConfig.class);
        }

        if (dbUnitConfig.getProperties().containsKey("escapePattern")) {
            if (dbUnitConfig.getProperties().get("escapePattern").equals("")) {
                //avoid Caused by: org.dbunit.DatabaseUnitRuntimeException: Empty string is an invalid escape pattern!
                // because @DBUnit annotation and dbunit.yml global config have escapePattern defaults to ""
                dbUnitConfig.getProperties().remove("escapePattern");
            }
        }
        instance.dbUnitConfig = dbUnitConfig;

    }

    public DBUnitConfig getDbUnitConfig() {
        return dbUnitConfig;
    }

}
