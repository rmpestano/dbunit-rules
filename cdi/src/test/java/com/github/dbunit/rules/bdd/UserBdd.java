package com.github.dbunit.rules.bdd;

import cucumber.api.CucumberOptions;
import cucumber.runtime.CdiCucumberTestRunner;
import org.junit.runner.RunWith;

/**
 * Created by rafael-pestano on 09/10/2015.
 */
@RunWith(CdiCucumberTestRunner.class)
@CucumberOptions(features = "src/test/resources/features/find-users.feature")
public class UserBdd {
}
