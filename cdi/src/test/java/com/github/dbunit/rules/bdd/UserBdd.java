package com.github.dbunit.rules.bdd;

import com.github.dbunit.rules.cucumber.CdiCucumberTestRunner;
import cucumber.api.CucumberOptions;
import org.junit.runner.RunWith;

/**
 * Created by rafael-pestano on 09/10/2015.
 */
@RunWith(CdiCucumberTestRunner.class)
@CucumberOptions(features = "src/test/resources/features/find-users.feature", plugin = "json:target/dbunit.json")
public class UserBdd {
}
