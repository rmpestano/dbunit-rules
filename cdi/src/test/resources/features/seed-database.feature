#order=1
Feature: Seeding database


Scenario: Seed database using dbunit CDI
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

#cukedoctor-discrete
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


And The following dataset located at src/test/resources/datasets/users.yml:
  """
user:
  - id: 1
    name: "@realpestano"
  - id: 2
    name: "@dbunit"
tweet:
  - id: abcdef12345
    content: "dbunit rules!"
    user_id: 1
  - id: abcdef12233
    content: "dbunit rules!"
    user_id: 2
  - id: abcdef1343
    content: "CDI for the win!"
    user_id: 2
follower:
  - id: 1
    user_id: 1
    follower_id: 2
  """

#cukedoctor-discrete
When The following test is executed:
 """
[source,java]
----
include::../../src/test/java/com/github/dbunit/rules/DBUnitCDITest.java[tags=seedDatabase]
----
 """

Then The database should be seeded with the dataset content