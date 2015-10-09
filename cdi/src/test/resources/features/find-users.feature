Feature: Find users

Scenario Outline: Find user by tweet
When When I search users who tweeted about "<tweet>"
Then I must find <number> of users
Examples:
| tweet  | number |
| dbunit | 2      |
| CDI    | 1      |
