# order: 5
Feature: Database assertion using expected datasets
====
[quote]
____
In order to verify database state after test execution
As a developer
I want to assert database state with datasets.
____
====

  Scenario: Database assertion with yml dataset

     #cukedoctor-discrete
    Given The following dataset
"""
.expectedUsers.yml
----
include::../../../core/src/test/resources/datasets/yml/expectedUsers.yml[]
----
"""

#cukedoctor-discrete
    When The following test is executed:
"""
[source,java,indent=0,linenums]
----
include::../../../core/src/test/java/com/github/dbunit/rules/ExpectedDataSetIt.java[tags=expectedDeclaration;expected]
----
"""
    Then Test must pass because database state is as in expected dataset.

  Scenario: Database assertion with regular expression in expected dataset

       #cukedoctor-discrete
    Given The following dataset
"""
.expectedUsersRegex.yml
----
include::../../../core/src/test/resources/datasets/yml/expectedUsersRegex.yml[]
----
"""

  #cukedoctor-discrete
    When The following test is executed:
"""
[source,java,indent=0,linenums]
----
include::../../../core/src/test/java/com/github/dbunit/rules/ExpectedDataSetIt.java[tags=expectedRegex]
----
"""
    Then Test must pass because database state is as in expected dataset.

  Scenario: Database assertion with seeding before test execution

       #cukedoctor-discrete
    Given The following dataset
"""
.user.yml
----
include::../../../core/src/test/resources/datasets/yml/user.yml[]
----
"""
    #cukedoctor-discrete
    And The following dataset
"""
.expectedUser.yml
----
include::../../../core/src/test/resources/datasets/yml/expectedUser.yml[]
----
"""

  #cukedoctor-discrete
    When The following test is executed:
"""
[source,java,indent=0,linenums]
----
include::../../../core/src/test/java/com/github/dbunit/rules/ExpectedDataSetIt.java[tags=expectedWithSeeding]
----
"""
    Then Test must pass because database state is as in expected dataset.

  Scenario: Failling database assertion

       #cukedoctor-discrete
    Given The following dataset
"""
.expectedUsers.yml
----
include::../../../core/src/test/resources/datasets/yml/expectedUsers.yml[]
----
"""

  #cukedoctor-discrete
    When The following test is executed:
"""
[source,java,indent=0,linenums]
----
include::../../../core/src/test/java/com/github/dbunit/rules/ExpectedDataSetIt.java[tags=faillingExpected]
----
"""

  #cukedoctor-discrete
    Then Test must fail with following error:
"""
IMPORTANT: junit.framework.ComparisonFailure: value (table=USER, row=0, col=name) expected:<[]expected user1> but was:<[non ]expected user1>  at org.dbunit.assertion.JUnitFailureFactory.createFailure(JUnitFailureFactory.java:39) at org.dbunit.assertion.DefaultFailureHandler.createFailure(DefaultFailureHandler.java:97) at org.dbunit.assertion.DefaultFailureHandler.handle(DefaultFailureHandler.java:223) at ...
"""