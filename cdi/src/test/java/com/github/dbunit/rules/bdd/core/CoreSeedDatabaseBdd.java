package com.github.dbunit.rules.bdd.core;

import com.github.dbunit.rules.cucumber.CdiCucumberTestRunner;
import cucumber.api.CucumberOptions;
import org.junit.runner.RunWith;

/**
 * Created by rafael-pestano on 09/10/2015.
 */
@RunWith(CdiCucumberTestRunner.class)
@CucumberOptions(features = "src/test/resources/features/core/core-seed-database.feature", plugin = "json:target/core-seed-database.json")
public class CoreSeedDatabaseBdd {
}
