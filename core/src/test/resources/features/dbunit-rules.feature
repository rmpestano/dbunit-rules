Feature: DBunit rules cucumber


    Scenario: List users
    Given The database is seeded with users
    When I list users
    Then 2 users must be found