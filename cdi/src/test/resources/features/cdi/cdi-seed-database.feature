#order=2
Feature: Seeding database with DBUnit Rules CDI module
[quote]
____
In order to manage database state in *CDI* based tests

As a developer

I want to use DBUnit in a CDI test environment
____


DBUnit CDI integration is done through a https://docs.jboss.org/weld/reference/latest/en-US/html_single/#interceptors[CDI interceptor^].

[IMPORTANT]
=====
CDI must be enabled in your test, see the following example:

[source, java]
----
\@RunWith(CdiTestRunner.class) <1>
public class DBUnitCDITest {

}
----
<1> https://deltaspike.apache.org/documentation/test-control.html[CdiTestRunner^] is provided by https://deltaspike.apache.org[Apache Deltaspike^] but you should be able to use other CDI test runners.
=====

Scenario: Seed database using yml dataset


#cukedoctor-discrete
#{[IMPORTANT]}
#{======}
#{Your test itself must be a CDI bean to be intercepted. if you’re using https://deltaspike.apache.org/documentation/test-control.html[Deltaspike test control^] just enable the following property in `test/resources/META-INF/apache-deltaspike.properties`:}
#{----}
#{deltaspike.testcontrol.use_test_class_as_cdi_bean=true}
#{----}
#{======}
Given DBUnit interceptor is enabled in your test beans.xml:
  """
.src/test/resources/META-INF/beans.xml
[source,xml]
----
<?xml version="1.0" encoding="UTF-8"?>
<beans xmlns="http://java.sun.com/xml/ns/javaee"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xsi:schemaLocation="http://java.sun.com/xml/ns/javaee http://java.sun.com/xml/ns/javaee/beans_1_0.xsd">

       <interceptors>
              <class>com.github.dbunit.rules.cdi.DBUnitInterceptor</class>
       </interceptors>
</beans>
----

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
include::../../src/test/java/com/github/dbunit/rules/DBUnitCDITest.java[tags=seedDatabase]
----
 """

Then The database should be seeded with the dataset content before test execution