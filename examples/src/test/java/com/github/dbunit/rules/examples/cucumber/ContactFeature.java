package com.github.dbunit.rules.examples.cucumber;

import com.github.dbunit.rules.cucumber.CdiCucumberTestRunner;
import cucumber.api.CucumberOptions;
import org.junit.runner.RunWith;


@RunWith(CdiCucumberTestRunner.class)
@CucumberOptions(
        features = {"src/test/resources/features/contacts.feature"},
        plugin = {"json:target/cucumber.json"}
        //glue = "com.github.dbunit.rules.examples.glues" <1>
)
public class ContactFeature {
}
