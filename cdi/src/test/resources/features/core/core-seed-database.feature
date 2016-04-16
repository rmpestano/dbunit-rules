#order=1
Feature: Seeding database with DBUnit Rules Core module
  [quote]
  ____
  In order to prepare database state during JUnit tests

  As a developer

  I want to integrate DBUnit in my JUnit tests.
  ____

DBUnit Rules Core module brings http://dbunit.sourceforge.net/[DBunit^] to your unit tests via https://github.com/junit-team/junit4/wiki/Rules[JUnit rules^].

  Scenario: Seed database using yml dataset

    #cukedoctor-discrete
    Given the following junit rules
    """
.src/test/resources/dataset/yml/users.yml
[source,java]
----
include::../../../core/src/test/java/com/github/dbunit/rules/DBUnitRulesIt.java[tags=rules]
----
    """
