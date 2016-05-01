# order: 3
Feature: Manage database with DBUnit Rules Cucumber
====
[quote]
____
In order to manage database state in `BDD` tests
As a BDD developer
I want to use DBUnit along side my BDD tests.
____
====

DBUnit enters the BDD world through a dedicated JUNit runner which is based on https://cucumber.io/[Cucumber^] and https://deltaspike.apache.org/[Apache DeltaSpike^].

This runner just starts CDI within your BDD tests so you just have to use <<Manage database with DBUnit Rules CDI,DBUnit rules CDI interceptor>> on Cucumber steps, here is the so called Cucumber CDI runner declaration:

[source,java]
----
include::../../src/test/java/com/github/dbunit/rules/bdd/DBUnitRulesBdd.java[]
----


IMPORTANT: As cucumber doesn't work with JUnit Rules, see https://github.com/cucumber/cucumber-jvm/issues/393[this issue^], so you *cannot* use cucumber runner with _DBunit Rules Core_ because its based on JUnit rules.

[discrete]
=== *Dependencies*
Here is a set of maven dependencies needed by DBUnit rules Cucumber:

NOTE: Most of the dependencies, except CDI container implementation, are bring by DBUnit Rules Cucumber module transitively.

.Cucumber dependencies
----
include::../../pom.xml[tags=cucumber-deps]
----
<1> You don't need to declare because it comes with DBUnit Rules Cucumber module dependency.

.DeltaSpike and CDI dependency
----
include::../../pom.xml[tags=deltaspike-cdi-deps]
----
<2> Also comes with DBUit Rules Cucumber.
<3> You can use CDI implementation of your choice.



Scenario: Seed database using DBUnit rules in Cucumber tests

#cukedoctor-discrete
Given The following feature
  """
----
include::../../../examples/src/test/resources/features/contacts.feature[]
----
  """


#cukedoctor-discrete
And The following dataset

 """
----
include::../../../examples/src/test/resources/datasets/contacts.yml[]
----
 """

#cukedoctor-discrete
And The following Cucumber test

 """
[source,java,linenums]
----
include::../../../examples/src/test/java/com/github/dbunit/rules/examples/cucumber/ContactFeature.java[]
----
 """

#cukedoctor-discrete
When The following cucumber steps are executed
 """
[source,java,linenums]
----
include::../../../examples/src/test/java/com/github/dbunit/rules/examples/cucumber/ContactSteps.java[]
----
<1> As the Cucumber cdi runner enables CDI, you can use injection into your Cucumber steps.
<2> Here we use the DBUnit Rules CDI interceptor to seed the database before step execution.
 """

Then The database should be seeded with the dataset content before step execution
