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
    Then Test must fail showing what it was expecting as database state.
"""
NOTE: This step is purposely failing to show the error message below.
"""