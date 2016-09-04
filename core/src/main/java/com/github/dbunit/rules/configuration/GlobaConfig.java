package com.github.dbunit.rules.configuration;

import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;

/**
 * Created by pestano on 03/09/16.
 *
 * pojo which represents dbunit.yml, used for global which can be overrided via @DataSet annotation
 * at class or method level and with @DBUnit at class or method level
 */
public class GlobaConfig {

    private static GlobaConfig instance;

    private DBUnitConfig dbUnitConfig;


    private GlobaConfig() {

    }

    public void setDbUnitConfig(DBUnitConfig dbUnitConfig) {
        this.dbUnitConfig = dbUnitConfig;
    }

    public static GlobaConfig instance() {
        if (instance == null) {
            createInstance();
        }
        if(instance.getDbUnitConfig().getProperties().containsKey("escapePattern")){
            if (instance.getDbUnitConfig().getProperties().get("escapePattern").equals("")){
                //avoid Caused by: org.dbunit.DatabaseUnitRuntimeException: Empty string is an invalid escape pattern!
                // because @DBUnit annotation and dbunit.yml global config have escapePattern defaults to ""
                instance.getDbUnitConfig().getProperties().remove("escapePattern");
            }
        }
        return instance;
    }

    public static GlobaConfig newInstance() {
        instance = null;
        return instance();
    }

    private static void createInstance() {
        //try to instance user provided dbunit.yml
        InputStream customConfiguration = Thread.currentThread().getContextClassLoader().getResourceAsStream("dbunit.yml");
        if(customConfiguration != null){
           instance = new Yaml().loadAs(customConfiguration, GlobaConfig.class);
        } else{
           //default config
           instance = new Yaml().loadAs(GlobaConfig.class.getResourceAsStream("/default/dbunit.yml"), GlobaConfig.class);
        }

    }

    public DBUnitConfig getDbUnitConfig() {
        return dbUnitConfig;
    }

}
