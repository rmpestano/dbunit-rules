package com.github.dbunit.rules.bdd;

import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import org.junit.runner.RunWith;

/**
 * Created by pestano on 23/07/15.
 */

@RunWith(Cucumber.class)
@CucumberOptions(features = "src/test/resources/features/dbunit-rules.feature", plugin = {"json:target/cucumber.json"} )
public class DBUnitRulesBdd {



}
