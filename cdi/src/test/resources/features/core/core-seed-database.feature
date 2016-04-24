# order: 1
Feature: Manage database with DBUnit Rules Core
====
[quote]
____
In order to manage database state in JUnit tests
As a developer
I want to use DBUnit in my tests.
____
====

DBUnit Rules Core module brings http://dbunit.sourceforge.net/[DBunit^] to your unit tests via https://github.com/junit-team/junit4/wiki/Rules[JUnit rules^].

  Scenario: Seed database using yml dataset

    #cukedoctor-discrete
    Given The following junit rules
    """
[source,java]
----
@RunWith(JUnit4.class)
public class DBUnitRulesIt {
include::../../../core/src/test/java/com/github/dbunit/rules/DBUnitRulesIt.java[tags=rules]
}
----
<1> https://github.com/rmpestano/dbunit-rules/blob/master/jpa/src/main/java/com/github/dbunit/rules/jpa/EntityManagerProvider.java[EntityManagerProvider^] is a simple Junit rule that creates a JPA entityManager for each test. DBunit rule don’t depend on EntityManagerProvider, it only needs a JDBC connection.
<2> DBUnit rule responsible for reading `@DataSet` annotation and prepare the database for each test.
    """

#cukedoctor-discrete
    And The following dataset

 """
.src/test/resources/dataset/yml/users.yml
----
include::../../src/test/resources/datasets/yml/users.yml[]
----
 """

#cukedoctor-discrete
    When The following test is executed:
 """
[source,java]
----
include::../../../core/src/test/java/com/github/dbunit/rules/DBUnitRulesIt.java[tags=seedDatabase]
----
 """
    Then The database should be seeded with the dataset content before test execution