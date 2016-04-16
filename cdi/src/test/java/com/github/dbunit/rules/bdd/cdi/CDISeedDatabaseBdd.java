package com.github.dbunit.rules.bdd.cdi;

import com.github.dbunit.rules.cucumber.CdiCucumberTestRunner;
import cucumber.api.CucumberOptions;
import org.junit.runner.RunWith;

/**
 * Created by rafael-pestano on 09/10/2015.
 */
@RunWith(CdiCucumberTestRunner.class)
@CucumberOptions(features = "src/test/resources/features/cdi/cdi-seed-database.feature", plugin = "json:target/cdi-seed-database.json")
public class CDISeedDatabaseBdd {
}
