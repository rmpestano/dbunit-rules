package com.github.dbunit.rules.examples.cucumber;

import com.github.dbunit.rules.cucumber.CdiCucumberTestRunner;
import cucumber.api.CucumberOptions;
import org.junit.runner.RunWith;

/**
 * Created by pestano on 12/02/16.
 */

@RunWith(CdiCucumberTestRunner.class)
@CucumberOptions(
        features = {"src/test/resources/features/contacts.feature"},
        plugin = {"json:target/cucumber.json"}
        //glue = "com.github.dbunit.rules.examples.glues" //step definitions can be in different packages for reuse
)
public class ContactFeature {
}
